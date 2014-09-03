/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark

import java.util.{List => JavaList}

import com.google.common.hash.Hashing
import org.apache.hadoop.hive.ql.cache.CacheManager

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.{Driver, QueryPlan}
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.log.PerfLogger
import org.apache.hadoop.hive.ql.metadata.AuthorizationException
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.{SerDe, SerDeUtils}
import org.apache.hadoop.util.StringUtils

import shark.api.TableRDD
import shark.api.QueryExecutionException
import shark.execution.{SharkDDLTask, SharkDDLWork}
import shark.execution.{SharkExplainTask, SharkExplainWork}
import shark.execution.{SparkLoadWork, SparkLoadTask}
import shark.execution.{SparkTask, SparkWork}
import shark.memstore2.ColumnarSerDe
import shark.parse.{QueryContext, SharkExplainSemanticAnalyzer, SharkSemanticAnalyzerFactory}
import shark.util.QueryRewriteUtils


/**
 * This static object is responsible for two things:
 * - Add Shark specific tasks to TaskFactory.taskvec.
 * - Use reflection to get access to private fields and methods in Hive Driver.
 *
 * See below for the SharkDriver class.
 */
private[shark] object SharkDriver extends LogHelper {

  // A dummy static method so we can make sure the following static code are executed.
  def runStaticCode() {
    logDebug("Initializing object SharkDriver")
  }

  def registerSerDe(serdeClass: Class[_ <: SerDe]) {
    SerDeUtils.registerSerDe(serdeClass.getName, serdeClass)
  }

  registerSerDe(classOf[ColumnarSerDe])

  // Task factory. Add Shark specific tasks.
  TaskFactory.taskvec.addAll(Seq(
    new TaskFactory.taskTuple(classOf[SharkDDLWork], classOf[SharkDDLTask]),
    new TaskFactory.taskTuple(classOf[SparkLoadWork], classOf[SparkLoadTask]),
    new TaskFactory.taskTuple(classOf[SparkWork], classOf[SparkTask]),
    new TaskFactory.taskTuple(classOf[SharkExplainWork], classOf[SharkExplainTask])))

  // Start the dashboard. Disabled by default. This was developed for the demo
  // at SIGMOD. We might turn it on later for general consumption.
  //dashboard.Dashboard.start()

  // Use reflection to make some private members accessible.
  val planField = classOf[Driver].getDeclaredField("plan")
  val contextField = classOf[Driver].getDeclaredField("ctx")
  val schemaField = classOf[Driver].getDeclaredField("schema")
  val errorMessageField = classOf[Driver].getDeclaredField("errorMessage")
  val logField = classOf[Driver].getDeclaredField("LOG")
  contextField.setAccessible(true)
  planField.setAccessible(true)
  schemaField.setAccessible(true)
  errorMessageField.setAccessible(true)
  logField.setAccessible(true)

  val doAuthMethod = classOf[Driver].getDeclaredMethod(
    "doAuthorization", classOf[BaseSemanticAnalyzer])
  doAuthMethod.setAccessible(true)
  val saHooksMethod = classOf[Driver].getDeclaredMethod(
    "getHooks", classOf[HiveConf.ConfVars], classOf[Class[_]])
  saHooksMethod.setAccessible(true)

  /**
   * Hold state variables specific to each query being executed, that may not
   * be consistent in the overall SessionState. Unfortunately this class was
   * a private static class in Driver. Too hard to use reflection ...
   */
  class QueryState {
    private var op: HiveOperation = _
    private var cmd: String = _
    private var init = false;

    def init(op: HiveOperation, cmd: String) {
      this.op = op;
      this.cmd = cmd;
      this.init = true;
    }

    def isInitialized(): Boolean = this.init
    def getOp = this.op
    def getCmd() = this.cmd
  }
}


/**
 * The driver to execute queries in Shark.
 */
private[shark] class SharkDriver(conf: HiveConf) extends Driver(conf) with LogHelper {

  // Helper methods to access the private members made accessible using reflection.
  def plan = getPlan
  def plan_= (value: QueryPlan): Unit = SharkDriver.planField.set(this, value)

  def context = SharkDriver.contextField.get(this).asInstanceOf[QueryContext]
  def context_= (value: QueryContext): Unit = SharkDriver.contextField.set(this, value)

  def schema = SharkDriver.schemaField.get(this).asInstanceOf[Schema]
  def schema_= (value: Schema): Unit = SharkDriver.schemaField.set(this, value)

  def errorMessage = SharkDriver.errorMessageField.get(this).asInstanceOf[String]
  def errorMessage_= (value: String): Unit = SharkDriver.errorMessageField.set(this, value)

  def LOG = SharkDriver.logField.get(null).asInstanceOf[org.apache.commons.logging.Log]

  var useTableRddSink = false

  override def init(): Unit = {
    // Forces the static code in SharkDriver to execute.
    SharkDriver.runStaticCode()

    // Init Hive Driver.
    super.init()
  }

  def tableRdd(cmd: String): Option[TableRDD] = {
    useTableRddSink = true
    val response = run(cmd)
    // Throw an exception if there is an error in query processing.
    if (response.getResponseCode() != 0) {
      throw new QueryExecutionException(response.getErrorMessage)
    }
    useTableRddSink = false
    plan.getRootTasks.get(0) match {
      case sparkTask: SparkTask => sparkTask.tableRdd
      case _ => None
    }
  }

  /**
   * Overload compile to use Shark's semantic analyzers.
   */
  override def compile(cmd: String, resetTaskIds: Boolean): Int = {
    val perfLogger: PerfLogger = PerfLogger.getPerfLogger()
    perfLogger.PerfLogBegin(LOG, PerfLogger.COMPILE)

    //holder for parent command type/string when executing reentrant queries
    val queryState = new SharkDriver.QueryState

    if (plan != null) {
      close()
      plan = null
    }

    if (resetTaskIds) {
      TaskFactory.resetId()
    }
    saveSession(queryState)

    try {
      val command = {
        val varSubbedCmd = new VariableSubstitution().substitute(conf, cmd).trim
        val cmdInUpperCase = varSubbedCmd.toUpperCase
        if (cmdInUpperCase.startsWith("CACHE")) {
          QueryRewriteUtils.cacheToAlterTable(varSubbedCmd)
        } else if (cmdInUpperCase.startsWith("UNCACHE")) {
          QueryRewriteUtils.uncacheToAlterTable(varSubbedCmd)
        } else {
          varSubbedCmd
        }
      }

      val ss: SessionState = SessionState.get
      val cacheManager: CacheManager = ss.getCacheManager
      val hc = Hashing.sha256.hashString(command).toString
      var isCached = false
      var isSelectQuery = false

      if(cacheManager != null) {
        if(cacheManager.isAsserted(hc)) {
          isCached = true
        }
        if(command.toUpperCase().startsWith("SELECT") || command.toUpperCase().startsWith("FROM")) {
          isSelectQuery = true;
        }
      }

      var tree: ASTNode = null
      var sem: BaseSemanticAnalyzer = null

      if(isCached) {
        println(hc + " : " + System.currentTimeMillis() + " - [START] Loading cached query data")
        context = cacheManager.get(hc).getContext.asInstanceOf[QueryContext]
        tree = cacheManager.get(hc).getTree
        sem = cacheManager.get(hc).getSem
        plan = cacheManager.get(hc).getQp
        plan.getFetchTask.resetTotalRows()
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Loading cached query data")
      } else {
        println(hc + " : " + System.currentTimeMillis() + " - [START] Create context")
        context = new QueryContext(conf, useTableRddSink)
        context.setCmd(command)
        context.setTryCount(getTryCount())
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Create context")

        println(hc + " : " + System.currentTimeMillis() + " - [START] Create tree")
        tree = ParseUtils.findRootNonNullToken((new ParseDriver()).parse(command, context))
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Create tree")
        println(hc + " : " + System.currentTimeMillis() + " - [START] Create sem")
        sem = SharkSemanticAnalyzerFactory.get(conf, tree)
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Create sem")

        if (!sem.isInstanceOf[ExplainSemanticAnalyzer] ||
          sem.isInstanceOf[SharkExplainSemanticAnalyzer]) {
          // Don't include the rewritten AST tree for Hive EXPLAIN mode.
          shark.parse.ASTRewriteUtil.countDistinctToGroupBy(tree)
        }

        // Do semantic analysis and plan generation
        val saHooks = SharkDriver.saHooksMethod.invoke(this, HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
          classOf[AbstractSemanticAnalyzerHook]).asInstanceOf[JavaList[AbstractSemanticAnalyzerHook]]
        if (saHooks != null) {
          val hookCtx = new HiveSemanticAnalyzerHookContextImpl()
          hookCtx.setConf(conf)
          saHooks.foreach(_.preAnalyze(hookCtx, tree))
          println(hc + " : " + System.currentTimeMillis() + " - [START] Analyze sem")
          sem.analyze(tree, context)
          println(hc + " : " + System.currentTimeMillis() + " - [END]   Analyze sem")
          hookCtx.update(sem)
          saHooks.foreach(_.postAnalyze(hookCtx, sem.getRootTasks()))
        } else {
          println(hc + " : " + System.currentTimeMillis() + " - [START] Analyze sem")
          sem.analyze(tree, context)
          println(hc + " : " + System.currentTimeMillis() + " - [END]   Analyze sem")
        }

        logDebug("Semantic Analysis Completed")

        println(hc + " : " + System.currentTimeMillis() + " - [START] Validate sem")
        sem.validate()
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Validate sem")

        println(hc + " : " + System.currentTimeMillis() + " - [START] Create plan")
        plan = new QueryPlan(command, sem,  perfLogger.getStartTime(PerfLogger.DRIVER_RUN))
        println(hc + " : " + System.currentTimeMillis() + " - [END]   Create plan")

        if(cacheManager != null && isSelectQuery) {
          println(hc + " : " + System.currentTimeMillis() + " - [START] Set cacheManager")
          cacheManager.get(hc).setContext(context)
          cacheManager.get(hc).setTree(tree)
          cacheManager.get(hc).setSem(sem)
          cacheManager.get(hc).setQp(plan)
          println(hc + " : " + System.currentTimeMillis() + " - [END]   Set cacheManager")
        }
      }

      // Initialize FetchTask right here. Somehow Hive initializes it twice...
      if (sem.getFetchTask != null) {
        sem.getFetchTask.initialize(conf, plan, null)
      }

      // get the output schema
      schema = Driver.getSchema(sem, conf)

      // skip the testing serialization code

      // do the authorization check
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
        try {
          perfLogger.PerfLogBegin(LOG, PerfLogger.DO_AUTHORIZATION)
          // Use reflection to invoke doAuthorization().
          SharkDriver.doAuthMethod.invoke(this, sem)
        } catch {
          case authExp: AuthorizationException => {
            logError("Authorization failed:" + authExp.getMessage()
              + ". Use show grant to get more details.")
            return 403
          }
        } finally {
          perfLogger.PerfLogEnd(LOG, PerfLogger.DO_AUTHORIZATION)
        }
      }

      // Success!
      0
    } catch {
      case e: SemanticException => {
        errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        10
      }
      case e: ParseException => {
        errorMessage = "FAILED: Parse Error: " + e.getMessage()
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        11
      }
      case e: Exception => {
        errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e)
        logError(errorMessage, "\n" + StringUtils.stringifyException(e))
        12
      }
    } finally {
      perfLogger.PerfLogEnd(LOG, PerfLogger.COMPILE)
      restoreSession(queryState)
    }
  }

  def saveSession(qs: SharkDriver.QueryState) {
    val oldss: SessionState = SessionState.get();
    if (oldss != null && oldss.getHiveOperation() != null) {
      qs.init(oldss.getHiveOperation(), oldss.getCmd())
    }
  }

  def restoreSession(qs: SharkDriver.QueryState) {
    val ss: SessionState = SessionState.get()
    if (ss != null && qs != null && qs.isInitialized()) {
      ss.setCmd(qs.getCmd())
      ss.setCommandType(qs.getOp)
    }
  }
}
