/*
 * Copyright 2019 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ResultSetReductionInterpreterStubs.h"
#include "CodeGenerator.h"
#include "ResultSetReductionCodegen.h"

namespace {

// Creates an empty stub function, with the fixed signature required by the interpreter.
llvm::Function* create_stub_function(const std::string& name, CgenState* cgen_state) {
  auto void_type = llvm::Type::getVoidTy(cgen_state->context_);
  auto int8_ptr_type = llvm::PointerType::get(get_int_type(8, cgen_state->context_), 0);
  std::vector<llvm::Type*> parameter_types(2, int8_ptr_type);
  const auto func_type = llvm::FunctionType::get(void_type, parameter_types, false);
  auto function = llvm::Function::Create(
      func_type, llvm::Function::ExternalLinkage, name, cgen_state->module_);
  const auto bb_entry =
      llvm::BasicBlock::Create(cgen_state->context_, ".entry", function, 0);
  cgen_state->ir_builder_.SetInsertPoint(bb_entry);
  return function;
}

// Returns the name of runtime function which reads the value wrapped by a
// ReductionInterpreter::EvalValue.
std::string get_stub_read_argument_name(const Type arg_type) {
  std::string read_arg_name{"read_stub_arg_"};
  switch (arg_type) {
    case Type::Int32:
    case Type::Int64: {
      read_arg_name += "int";
      break;
    }
    case Type::Float: {
      read_arg_name += "float";
      break;
    }
    case Type::Double: {
      read_arg_name += "double";
      break;
    }
    case Type::Int8Ptr: {
      read_arg_name += "pi8";
      break;
    }
    case Type::Int32Ptr: {
      read_arg_name += "pi32";
      break;
    }
    case Type::Int64Ptr: {
      read_arg_name += "pi64";
      break;
    }
    case Type::VoidPtr: {
      read_arg_name += "pvoid";
      break;
    }
    case Type::Int64PtrPtr: {
      read_arg_name += "ppi64";
      break;
    }
    default: {
      LOG(FATAL) << "Invalid type: " << static_cast<int>(arg_type);
    }
  }
  return read_arg_name;
}

}  // namespace

bool is_integer_type(const Type type) {
  switch (type) {
    case Type::Int1:
    case Type::Int8:
    case Type::Int32:
    case Type::Int64: {
      return true;
    }
    default: {
      return false;
    }
  }
}

bool is_pointer_type(const Type type) {
  switch (type) {
    case Type::Int8Ptr:
    case Type::Int32Ptr:
    case Type::Int64Ptr:
    case Type::FloatPtr:
    case Type::DoublePtr:
    case Type::VoidPtr:
    case Type::Int64PtrPtr: {
      return true;
    }
    default: {
      return false;
    }
  }
}

// The following read_stub_arg_* functions read the argument at the given position from
// the list of wrapped inputs passed from the interpreter.

extern "C" int64_t read_stub_arg_int(const void* inputs_handle, const int32_t i) {
  const auto& inputs = *reinterpret_cast<const StubGenerator::InputsType*>(inputs_handle);
  CHECK_LT(static_cast<size_t>(i), inputs.size());
  return inputs[i].int_val;
}

extern "C" float read_stub_arg_float(const void* inputs_handle, const int32_t i) {
  const auto& inputs = *reinterpret_cast<const StubGenerator::InputsType*>(inputs_handle);
  CHECK_LT(static_cast<size_t>(i), inputs.size());
  return inputs[i].float_val;
}

extern "C" double read_stub_arg_double(const void* inputs_handle, const int32_t i) {
  const auto& inputs = *reinterpret_cast<const StubGenerator::InputsType*>(inputs_handle);
  CHECK_LT(static_cast<size_t>(i), inputs.size());
  return inputs[i].double_val;
}

extern "C" const void* read_stub_arg_pvoid(const void* inputs_handle, const int32_t i) {
  const auto& inputs = *reinterpret_cast<const StubGenerator::InputsType*>(inputs_handle);
  CHECK_LT(static_cast<size_t>(i), inputs.size());
  return inputs[i].ptr;
}

extern "C" const int8_t* read_stub_arg_pi8(const void* inputs_handle, const int32_t i) {
  return static_cast<const int8_t*>(read_stub_arg_pvoid(inputs_handle, i));
}

extern "C" const int32_t* read_stub_arg_pi32(const void* inputs_handle, const int32_t i) {
  return static_cast<const int32_t*>(read_stub_arg_pvoid(inputs_handle, i));
}

extern "C" const int32_t* read_stub_arg_pi64(const void* inputs_handle, const int32_t i) {
  return static_cast<const int32_t*>(read_stub_arg_pvoid(inputs_handle, i));
}

extern "C" const int64_t* const* read_stub_arg_ppi64(const void* inputs_handle,
                                                     const int32_t i) {
  return static_cast<const int64_t* const*>(read_stub_arg_pvoid(inputs_handle, i));
}

// Writes back the value returned by the runtime function to the wrapped output value used
// from the interpreter.
extern "C" void write_stub_result_int(void* output_handle, const int64_t int_val) {
  auto output = reinterpret_cast<ReductionInterpreter::EvalValue*>(output_handle);
  output->int_val = int_val;
}

// Generates a stub function with a fixed signature which can be called from the
// interpreter. The generated function extracts the values from the list of wrapped values
// from the interpreter, consistent with the provided argument types.
StubGenerator::Stub StubGenerator::generateStub(const std::string& name,
                                                const std::vector<Type>& arg_types,
                                                const Type ret_type,
                                                const bool is_external) {
  const auto stub_name = name + "_stub";
  CodeCacheKey key{stub_name};
  std::lock_guard<std::mutex> s_stubs_cache_lock(s_stubs_cache_mutex);
  const auto val_ptr = s_stubs_cache.get(key);
  if (val_ptr) {
    return reinterpret_cast<StubGenerator::Stub>(std::get<0>(val_ptr->first.front()));
  }
  auto cgen_state = std::make_unique<CgenState>(std::vector<InputTableInfo>{}, false);
  std::unique_ptr<llvm::Module> module(runtime_module_shallow_copy(cgen_state.get()));
  cgen_state->module_ = module.get();
  const auto function = create_stub_function(stub_name, cgen_state.get());
  CHECK(function);
  auto& ctx = cgen_state->context_;
  std::vector<llvm::Value*> callee_args;
  auto inputs_it = function->arg_begin() + 1;
  for (size_t i = 0; i < arg_types.size(); ++i) {
    const auto arg_type = arg_types[i];
    const auto read_arg_name = get_stub_read_argument_name(arg_type);
    const auto llvm_arg_type = llvm_type(arg_type, ctx);
    auto callee_arg = cgen_state->emitExternalCall(
        read_arg_name, llvm_arg_type, {&*inputs_it, cgen_state->llInt<int32_t>(i)});
    if (is_integer_type(arg_type)) {
      CHECK(llvm_arg_type->isIntegerTy());
      callee_arg = cgen_state->ir_builder_.CreateTrunc(callee_arg, llvm_arg_type);
    } else if (is_pointer_type(arg_type)) {
      CHECK(llvm_arg_type->isPointerTy());
      callee_arg = cgen_state->ir_builder_.CreateBitCast(callee_arg, llvm_arg_type);
    }
    callee_args.push_back(callee_arg);
  }
  const auto llvm_ret_type = llvm_type(ret_type, ctx);
  auto value = is_external
                   ? cgen_state->emitExternalCall(name, llvm_ret_type, callee_args)
                   : cgen_state->emitCall(name, callee_args);
  auto output = &*(function->arg_begin());
  auto void_type = llvm::Type::getVoidTy(ctx);
  std::string write_arg_name{"write_stub_result_"};
  switch (ret_type) {
    case Type::Int8: {
      write_arg_name += "int";
      const auto i64_type = get_int_type(64, cgen_state->context_);
      value = cgen_state->ir_builder_.CreateSExt(value, i64_type);
      break;
    }
    case Type::Int32: {
      write_arg_name += "int";
      const auto i64_type = get_int_type(64, cgen_state->context_);
      value = cgen_state->ir_builder_.CreateSExt(value, i64_type);
      break;
    }
    case Type::Void: {
      value = nullptr;
      break;
    }
    default: {
      LOG(FATAL) << "Invalid type: " << static_cast<int>(ret_type);
    }
  }
  if (value) {
    cgen_state->emitExternalCall(write_arg_name, void_type, {output, value});
  }
  cgen_state->ir_builder_.CreateRetVoid();
  verify_function_ir(function);
  CompilationOptions co{
      ExecutorDeviceType::CPU, false, ExecutorOptLevel::ReductionJIT, false};
  module.release();
  auto ee = CodeGenerator::generateNativeCPUCode(function, {function}, co);
  auto func_ptr =
      reinterpret_cast<StubGenerator::Stub>(ee->getPointerToFunction(function));
  auto cache_val = std::make_tuple(reinterpret_cast<void*>(func_ptr), std::move(ee));
  std::vector<std::tuple<void*, ExecutionEngineWrapper>> cache_vals;
  cache_vals.emplace_back(std::move(cache_val));
  Executor::addCodeToCache(
      key, std::move(cache_vals), function->getParent(), s_stubs_cache);
  return func_ptr;
}

void StubGenerator::clearCache() {
  std::lock_guard<std::mutex> s_stubs_cache_lock(s_stubs_cache_mutex);
  s_stubs_cache.clear();
}

CodeCache StubGenerator::s_stubs_cache(10000);
std::mutex StubGenerator::s_stubs_cache_mutex;