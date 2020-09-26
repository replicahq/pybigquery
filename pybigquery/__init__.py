# Copyright (c) 2017 The PyBigQuery Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.sql import expression, operators, sqltypes

__all__ = ["array", "struct"]


class STRUCT(sqltypes.Indexable, sqltypes.TypeEngine):
    # NOTE: STRUCT names/types aren't currently supported.

    __visit_name__ = "STRUCT"

    class Comparator(sqltypes.Indexable.Comparator):
        def _setup_getitem(self, index):
            return operators.getitem, index, self.type

    comparator_factory = Comparator


class struct(expression.ClauseList, expression.ColumnElement):
    """ Create a BigQuery struct literal from a collection of named expressions/clauses.
    """
    # NOTE: Struct subfields aren't currently propagated/validated.

    __visit_name__ = "struct"

    def __init__(self, clauses, field=None, **kw):
        self.field = field
        self.type = STRUCT()
        super().__init__(*clauses, **kw)

    def _bind_param(self, operator, obj, _assume_scalar=False, type_=None):
        if operator is operators.getitem:
            # TODO:
            # - Validate field in clauses (or error if no clauses)
            # - If the field is a sub-struct, return with all clauses, otherwise none.
            return struct([], field=obj)

    def self_group(self, against=None):
        if not self.field and against in (operators.getitem,):
            return expression.Grouping(self)
        else:
            return self
