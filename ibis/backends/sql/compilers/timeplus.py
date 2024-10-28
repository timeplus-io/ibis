from __future__ import annotations

import math

import sqlglot.expressions as sge

import ibis.common.exceptions as com
import ibis.expr.operations as ops
from ibis.backends.sql.compilers.base import AggGen, SQLGlotCompiler
from ibis.backends.sql.datatypes import TimeplusType
from ibis.backends.sql.dialects import Timeplus

STAR = sge.Star()


class TimeplusCompiler(SQLGlotCompiler):
    __slots__ = ()

    type_mapper = TimeplusType

    dialect = Timeplus

    agg = AggGen(supports_filter=True)

    LOWERED_OPS = {ops.Capitalize: None}

    UNSUPPORTED_OPS = (
        ops.ApproxMedian,
        ops.ArgMax,
        ops.ArgMin,
        ops.ArrayDistinct,
        ops.ArrayFilter,
        ops.ArrayFlatten,
        ops.ArrayIntersect,
        ops.ArrayMap,
        ops.ArraySort,
        ops.ArrayUnion,
        ops.ArrayZip,
        ops.CountDistinctStar,
        ops.Covariance,
        ops.Date,
        ops.DateDelta,
        ops.DateFromYMD,
        ops.DayOfWeekIndex,
        ops.DayOfWeekName,
        ops.IntervalFromInteger,
        ops.IsNan,
        ops.IsInf,
        ops.Levenshtein,
        ops.Median,
        ops.RandomUUID,
        ops.RegexReplace,
        ops.RegexSplit,
        ops.RowID,
        ops.StandardDev,
        ops.Strftime,
        ops.StringAscii,
        ops.StringSplit,
        ops.StringToDate,
        ops.StringToTimestamp,
        ops.TimeDelta,
        ops.TimestampBucket,
        ops.TimestampDelta,
        ops.Translate,
        ops.TypeOf,
        ops.Unnest,
        ops.Variance,
    )

    SIMPLE_OPS = {
        ops.All: "min",
        ops.Any: "max",
    }

    @staticmethod
    def _generate_groups(groups):
        return groups

    def visit_NonNullLiteral(self, op, *, value, dtype):
        if dtype.is_inet():
            v = str(value)
            return self.f.to_ipv6(v) if ":" in v else self.f.to_ipv4(v)
        elif dtype.is_decimal():
            precision = dtype.precision
            if precision is None or not 1 <= precision <= 76:
                raise NotImplementedError(
                    f"Unsupported precision. Supported values: [1 : 76]. Current value: {precision!r}"
                )

            if 1 <= precision <= 9:
                type_name = self.f.to_decimal32
            elif 10 <= precision <= 18:
                type_name = self.f.to_decimal64
            elif 19 <= precision <= 38:
                type_name = self.f.to_decimal128
            else:
                type_name = self.f.to_decimal256
            return type_name(value, dtype.scale)
        elif dtype.is_numeric():
            if not math.isfinite(value):
                return sge.Literal.number(str(value))
            return sge.convert(value)
        elif dtype.is_interval():
            if dtype.unit.short in ("ms", "us", "ns"):
                raise com.UnsupportedOperationError(
                    "Timeplus doesn't support subsecond interval resolutions"
                )

            return sge.Interval(
                this=sge.convert(str(value)), unit=dtype.resolution.upper()
            )
        elif dtype.is_timestamp():
            funcname = "parse_datetime"

            if micros := value.microsecond:
                funcname += "64"

            funcname += "BestEffort"

            args = [value.isoformat()]

            if micros % 1000:
                args.append(6)
            elif micros // 1000:
                args.append(3)

            if (timezone := dtype.timezone) is not None:
                args.append(timezone)

            return self.f[funcname](*args)
        elif dtype.is_date():
            return self.f.to_date(value.isoformat())
        elif dtype.is_array():
            value_type = dtype.value_type
            values = [
                self.visit_Literal(
                    ops.Literal(v, dtype=value_type), value=v, dtype=value_type
                )
                for v in value
            ]
            return self.f.array(*values)
        elif dtype.is_map():
            value_type = dtype.value_type
            keys = []
            values = []

            for k, v in value.items():
                keys.append(sge.convert(k))
                values.append(
                    self.visit_Literal(
                        ops.Literal(v, dtype=value_type),
                        value=v,
                        dtype=value_type,
                    )
                )

            return self.f.map(self.f.array(*keys), self.f.array(*values))
        elif dtype.is_struct():
            fields = [
                self.visit_Literal(
                    ops.Literal(v, dtype=field_type), value=v, dtype=field_type
                )
                for field_type, v in zip(dtype.types, value.values())
            ]
            return self.f.tuple(*fields)
        else:
            return None


compiler = TimeplusCompiler()
