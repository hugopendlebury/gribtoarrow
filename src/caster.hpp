#pragma once

#include <arrow/api.h>
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>


namespace pybind11 { namespace detail {
    template <typename RecordBatch> struct gen_type_caster {
    public:
        PYBIND11_TYPE_CASTER(std::shared_ptr<RecordBatch>, _("pyarrow::RecordBatch"));
        // Python -> C++
        bool load(handle src, bool) {
            PyObject *source = src.ptr();
            if (!arrow::py::is_batch(source))
                return false;
            arrow::Result<std::shared_ptr<arrow::RecordBatch>> result = arrow::py::unwrap_batch(source);
            if(!result.ok())
                return false;
            value = std::static_pointer_cast<RecordBatch>(result.ValueOrDie());
            return true;
        }
        // C++ -> Python
        static handle cast(std::shared_ptr<RecordBatch> src, return_value_policy /* policy */, handle /* parent */) {
            return arrow::py::wrap_batch(src);
        }

    };
    template <>
    struct type_caster<std::shared_ptr<arrow::RecordBatch>> : public gen_type_caster<arrow::RecordBatch> {
    };


    template <typename Table> struct table_type_caster {
    public:
        PYBIND11_TYPE_CASTER(std::shared_ptr<arrow::Table>, _("pyarrow::Table"));
        // Python -> C++
        bool load(handle src, bool) {
            PyObject *source = src.ptr();
            if (!arrow::py::is_table(source))
                return false;
            arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::py::unwrap_table(source);
            if(!result.ok())
                return false;
            value = std::static_pointer_cast<arrow::Table>(result.ValueOrDie());
            return true;
        }
        // C++ -> Python
        static handle cast(std::shared_ptr<arrow::Table> src, return_value_policy , handle ) {
            return arrow::py::wrap_table(src);
        }
    };
    template <>
    struct type_caster<std::shared_ptr<arrow::Table>> : public table_type_caster<arrow::Table> {
    };
}}

