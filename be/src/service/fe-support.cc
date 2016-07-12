// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains implementations for the JNI FeSupport interface.

#include "service/fe-support.h"
#include "service/fe-support-common.h"

#include <boost/scoped_ptr.hpp>

#include "common/names.h"

using namespace impala;
using namespace apache::thrift::server;

// Called from the FE when it explicitly loads libfesupport.so for tests.
// This creates the minimal state necessary to service the other JNI calls.
// This is not called when we first start up the BE.
extern "C"
JNIEXPORT void JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeFeTestInit(
    JNIEnv* env, jclass caller_class) {
  DCHECK(ExecEnv::GetInstance() == NULL) << "This should only be called once from the FE";
  char* name = const_cast<char*>("FeSupport");
  InitCommonRuntime(1, &name, false, TestInfo::FE_TEST);
  LlvmCodeGen::InitializeLlvm(true);
  ExecEnv* exec_env = new ExecEnv(); // This also caches it from the process.
  exec_env->InitForFeTests();
}

// Evaluates a batch of const exprs and returns the results in a serialized
// TResultRow, where each TColumnValue in the TResultRow stores the result of
// a predicate evaluation. It requires JniUtil::Init() to have been
// called.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeEvalConstExprs(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_expr_batch,
    jbyteArray thrift_query_ctx_bytes) {
  jbyteArray result_bytes = NULL;
  TQueryCtx query_ctx;
  TExprBatch expr_batch;
  JniLocalFrame jni_frame;
  TResultRow expr_results;
  ObjectPool obj_pool;

  DeserializeThriftMsg(env, thrift_expr_batch, &expr_batch);
  DeserializeThriftMsg(env, thrift_query_ctx_bytes, &query_ctx);
  query_ctx.request.query_options.disable_codegen = true;
  RuntimeState state(query_ctx);

  THROW_IF_ERROR_RET(jni_frame.push(env), env, JniUtil::internal_exc_class(),
                     result_bytes);
  // Exprs can allocate memory so we need to set up the mem trackers before
  // preparing/running the exprs.
  state.InitMemTrackers(TUniqueId(), NULL, -1);

  vector<TExpr>& texprs = expr_batch.exprs;
  // Prepare the exprs
  vector<ExprContext*> expr_ctxs;
  for (vector<TExpr>::iterator it = texprs.begin(); it != texprs.end(); it++) {
    ExprContext* ctx;
    THROW_IF_ERROR_RET(Expr::CreateExprTree(&obj_pool, *it, &ctx), env,
                       JniUtil::internal_exc_class(), result_bytes);
    THROW_IF_ERROR_RET(ctx->Prepare(&state, RowDescriptor(), state.query_mem_tracker()),
                       env, JniUtil::internal_exc_class(), result_bytes);
    expr_ctxs.push_back(ctx);
  }

  if (state.codegen_created()) {
    // Finalize the module so any UDF functions are jit'd
    LlvmCodeGen* codegen = NULL;
    state.GetCodegen(&codegen, /* initialize */ false);
    DCHECK(codegen != NULL);
    codegen->EnableOptimizations(false);
    codegen->FinalizeModule();
  }

  vector<TColumnValue> results;
  // Open and evaluate the exprs. Also, always Close() the exprs even in case of errors.
  for (int i = 0; i < expr_ctxs.size(); ++i) {
    Status open_status = expr_ctxs[i]->Open(&state);
    if (!open_status.ok()) {
      for (int j = i; j < expr_ctxs.size(); ++j) {
        expr_ctxs[j]->Close(&state);
      }
      (env)->ThrowNew(JniUtil::internal_exc_class(), open_status.GetDetail().c_str());
      return result_bytes;
    }
    TColumnValue val;
    expr_ctxs[i]->GetValue(NULL, false, &val);
    // We check here if an error was set in the expression evaluated through GetValue()
    // and throw an exception accordingly
    Status getvalue_status = expr_ctxs[i]->root()->GetFnContextError(expr_ctxs[i]);
    if (!getvalue_status.ok()) {
      for (int j = i; j < expr_ctxs.size(); ++j) {
        expr_ctxs[j]->Close(&state);
      }
      (env)->ThrowNew(JniUtil::internal_exc_class(), getvalue_status.GetDetail().c_str());
      return result_bytes;
    }

    expr_ctxs[i]->Close(&state);
    results.push_back(val);
  }

  expr_results.__set_colVals(results);
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &expr_results, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeCacheJar(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TCacheJarParams params;
  DeserializeThriftMsg(env, thrift_struct, &params);

  TCacheJarResult result;
  string local_path;
  Status status = LibCache::instance()->GetLocalLibPath(params.hdfs_location,
      LibCache::TYPE_JAR, &local_path);
  status.ToThrift(&result.status);
  if (status.ok()) result.__set_local_path(local_path);

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeLookupSymbol(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TSymbolLookupParams lookup;
  DeserializeThriftMsg(env, thrift_struct, &lookup);

  vector<ColumnType> arg_types;
  for (int i = 0; i < lookup.arg_types.size(); ++i) {
    arg_types.push_back(ColumnType::FromThrift(lookup.arg_types[i]));
  }

  TSymbolLookupResult result;
  ResolveSymbolLookup(lookup, arg_types, &result);

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

// Calls in to the catalog server to request prioritizing the loading of metadata for
// specific catalog objects.
extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativePrioritizeLoad(
    JNIEnv* env, jclass caller_class, jbyteArray thrift_struct) {
  TPrioritizeLoadRequest request;
  DeserializeThriftMsg(env, thrift_struct, &request);

  CatalogOpExecutor catalog_op_executor(ExecEnv::GetInstance(), NULL, NULL);
  TPrioritizeLoadResponse result;
  Status status = catalog_op_executor.PrioritizeLoad(request, &result);
  if (!status.ok()) {
    LOG(ERROR) << status.GetDetail();
    // Create a new Status, copy in this error, then update the result.
    Status catalog_service_status(result.status);
    catalog_service_status.MergeStatus(status);
    status.ToThrift(&result.status);
  }

  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &result, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

extern "C"
JNIEXPORT jbyteArray JNICALL
Java_com_cloudera_impala_service_FeSupport_NativeGetStartupOptions(JNIEnv* env,
    jclass caller_class) {
  TStartupOptions options;
  ExecEnv* exec_env = ExecEnv::GetInstance();
  ImpalaServer* impala_server = exec_env->impala_server();
  options.__set_compute_lineage(impala_server->IsLineageLoggingEnabled());
  jbyteArray result_bytes = NULL;
  THROW_IF_ERROR_RET(SerializeThriftMsg(env, &options, &result_bytes), env,
                     JniUtil::internal_exc_class(), result_bytes);
  return result_bytes;
}

namespace impala {

static JNINativeMethod native_methods[] = {
  {
    (char*)"NativeFeTestInit", (char*)"()V",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativeFeTestInit
  },
  {
    (char*)"NativeEvalConstExprs", (char*)"([B[B)[B",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativeEvalConstExprs
  },
  {
    (char*)"NativeCacheJar", (char*)"([B)[B",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativeCacheJar
  },
  {
    (char*)"NativeLookupSymbol", (char*)"([B)[B",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativeLookupSymbol
  },
  {
    (char*)"NativePrioritizeLoad", (char*)"([B)[B",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativePrioritizeLoad
  },
  {
    (char*)"NativeGetStartupOptions", (char*)"()[B",
    (void*)::Java_com_cloudera_impala_service_FeSupport_NativeGetStartupOptions
  },
};

void InitFeSupport() {
  JNIEnv* env = getJNIEnv();
  jclass native_backend_cl = env->FindClass("com/cloudera/impala/service/FeSupport");
  env->RegisterNatives(native_backend_cl, native_methods,
      sizeof(native_methods) / sizeof(native_methods[0]));
  EXIT_IF_EXC(env);
}

}
