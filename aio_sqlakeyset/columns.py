"""
Classes and supporting functions to manipulate ordering columns and extract
keyset markers from query results.
"""
from copy import copy
from typing import List, Optional, Union
from warnings import warn

from sqlalchemy import asc, column
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Bundle, Mapper, class_mapper
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy.orm.exc import UnmappedColumnError
from sqlalchemy.sql.elements import ClauseList, Label, _label_reference
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.operators import asc_op, desc_op, nullsfirst_op, nullslast_op

_LABELLED = (Label, _label_reference)
_ORDER_MODIFIERS = (asc_op, desc_op, nullsfirst_op, nullslast_op)
_UNSUPPORTED_ORDER_MODIFIERS = (nullsfirst_op, nullslast_op)
_WRAPPING_DEPTH = 1000
_WRAPPING_OVERFLOW = (
    "Maximum element wrapping depth reached; there's "
    "probably a circularity in sqlalchemy that "
    "sqlakeyset doesn't know how to handle."
)


def order_by_clauses(selectable):
    """Extract the ORDER BY clause list from a select/query"""
    return selectable._order_by_clauses


def parse_ob_clause(selectable, backwards: bool):
    """Parse the ORDER BY clause of a selectable into a list of :class:`OC` instances."""

    def _flatten(cl):
        if isinstance(cl, ClauseList):
            for subclause in cl.clauses:
                yield from _flatten(subclause)
        elif isinstance(cl, (tuple, list)):
            for xs in cl:
                yield from _flatten(xs)
        else:
            yield cl

    result = [OC(c) for c in _flatten(order_by_clauses(selectable))]
    if backwards:
        result = [c.reversed for c in result]
    return result


def _warn_if_nullable(x):
    try:
        if x.nullable or x.property.columns[0].nullable:
            warn(
                "Ordering by nullable column {} can cause rows to be "
                "incorrectly omitted from the results. "
                "See the sqlakeyset README for more details.".format(x),
                stacklevel=7,
            )
            # stacklevel makes the warning appear in the user's calling code:
            # 1 _warn_if_nullable
            # 2 OC.__init__
            # 3 list comprehension in parse_clause
            # 4 parse_clause
            # 5 perform_paging
            # 6 get_page
            # 7 <user code>
    except (AttributeError, IndexError, KeyError):
        # x isn't a column, it's probably an expression or something
        pass


class OC:
    """
    Wrapper class for ordering columns; i.e.  instances of
    :class:`sqlalchemy.sql.expression.ColumnElement` appearing in the ORDER BY
    clause of a query we are paging.
    """

    def __init__(self, x):
        if isinstance(x, str):
            x = column(x)
        if _get_order_direction(x) is None:
            x = asc(x)
        self.uo = x
        _warn_if_nullable(self.comparable_value)
        self.full_name = str(self.element)
        try:
            table_name, name = self.full_name.split(".", 1)
        except ValueError:
            table_name = None  # type: ignore
            name = self.full_name

        self.table_name = table_name
        self.name = name

    @property
    def quoted_full_name(self):
        return str(self).split()[0]

    @property
    def element(self):
        """The ordering column/SQL expression with ordering modifier removed."""
        return _remove_order_direction(self.uo)

    @property
    def comparable_value(self):
        """
        The ordering column/SQL expression in a form that is suitable for
        incorporating in a ``ROW(...) > ROW(...)`` comparision; i.e. with ordering
        modifiers and labels removed.
        """
        return strip_labels(self.element)

    @property
    def is_ascending(self):
        """
        Returns ``True`` if this column is ascending, ``False`` if
        descending.

        Raises:
            ValueError: ???
        """
        d = _get_order_direction(self.uo)
        if d is None:
            raise ValueError
        return d == asc_op

    @property
    def reversed(self):
        """
        An :class:`OC` representing the same column ordering, but reversed.

        Raises:
            ValueError: ???
        """
        new_uo = _reverse_order_direction(self.uo)
        if new_uo is None:
            raise ValueError
        return OC(new_uo)

    def pair_for_comparison(self, value, dialect):
        """
        Return a pair of SQL expressions representing comparable values for
        this ordering column and a specified value.

        :param value: A value to compare this column against.
        :param dialect: The :class:`sqlalchemy.engine.interfaces.Dialect` in
            use.
        :returns: A pair `(a, b)` such that the comparison `a < b` is the
            condition for the value of this OC being past `value` in the paging
            order.
        """
        compval = self.comparable_value
        # If this OC is a column with a custom type, apply the custom
        # preprocessing to the comparsion value:
        try:
            value = compval.type.bind_processor(dialect)(value)  # type: ignore
        except (TypeError, AttributeError):
            pass
        if self.is_ascending:
            return compval, value
        else:
            return value, compval

    def __str__(self):
        return str(self.uo)

    def __repr__(self):
        return f"<OC: {str(self)}>"


def strip_labels(el):
    """
    Remove labels from a
    :class:`sqlalchemy.sql.expression.ColumnElement`.

    Raises:
        ValueError: ???
    """
    while isinstance(el, _LABELLED):
        try:
            el = el.element
        except AttributeError:
            raise ValueError
    return el


def _get_order_direction(x):
    """
    Given a :class:`sqlalchemy.sql.expression.ColumnElement`, find and return
    its ordering direction (ASC or DESC) if it has one.

    :param x: a :class:`sqlalchemy.sql.expression.ColumnElement`
    :return: `asc_op`, `desc_op` or `None`

    Raises:
        Exception: ???
    """
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, "modifier", None)
        if mod in (asc_op, desc_op):
            return mod

        el = getattr(x, "element", None)
        if el is None:
            return None
        x = el
    raise Exception(_WRAPPING_OVERFLOW)


def _reverse_order_direction(ce):
    """
    Given a :class:`sqlalchemy.sql.expression.ColumnElement`, return a copy
    with its ordering direction (ASC or DESC) reversed (if it has one).

    :param ce: a :class:`sqlalchemy.sql.expression.ColumnElement`

    Raises:
        Exception: ???
    """
    x = copied = ce._clone()
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, "modifier", None)
        if mod in (asc_op, desc_op):
            if mod == asc_op:
                x.modifier = desc_op
            else:
                x.modifier = asc_op
            return copied
        else:
            if not hasattr(x, "element"):
                return copied
            # Since we're going to change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)  # pragma: no cover


def _remove_order_direction(ce):
    """
    Given a :class:`sqlalchemy.sql.expression.ColumnElement`, return a copy
    with its ordering modifiers (ASC/DESC, NULLS FIRST/LAST) removed (if it has
    any).

    :param ce: a :class:`sqlalchemy.sql.expression.ColumnElement`

    Raises:
        Exception: ???
    """
    x = copied = ce._clone()
    parent = None
    for _ in range(_WRAPPING_DEPTH):
        mod = getattr(x, "modifier", None)
        if mod in _UNSUPPORTED_ORDER_MODIFIERS:
            warn(
                "One of your order columns had a NULLS FIRST or NULLS LAST "
                "modifier; but sqlakeyset does not support order columns "
                "with nulls. YOUR RESULTS WILL BE WRONG. See the "
                "Limitations section of the sqlakeyset README for more "
                "information."
            )
        if mod in _ORDER_MODIFIERS:
            x._copy_internals()
            if parent is None:
                # The modifier was at the top level; so just take the child.
                copied = x = x.element
            else:
                # Remove this link from the wrapping element chain and return
                # the top-level expression.
                parent.element = x = x.element  # type: ignore
        else:
            if not hasattr(x, "element"):
                return copied
            parent = x
            # Since we might change something inside x.element, we
            # need to clone another level deeper.
            x._copy_internals()
            x = x.element
    raise Exception(_WRAPPING_OVERFLOW)  # pragma: no cover


class MappedOrderColumn:
    """
    An ordering column in the context of a particular query/select.

    This wraps an :class:`OC` with one extra piece of information: how to
    retrieve the value of the ordering key from a result row. For some queries,
    this requires adding extra entities to the query; in this case,
    ``extra_column`` will be set.
    """

    def __init__(self, oc: OC):
        self.oc = oc
        self.extra_column = None
        """An extra SQLAlchemy ORM entity that this ordering column needs to
        add to its query in order to retrieve its value at each row. If no
        extra data is required, the value of this property will be ``None``."""

    def get_from_row(self, internal_row):
        """
        Extract the value of this ordering column from a result row.

        Raises:
            NotImplementedError: If not overridden.
        """
        raise NotImplementedError

    @property
    def ob_clause(self):
        """The original ORDER BY (sub)clause underlying this column."""
        return self.oc.uo

    @property
    def reversed(self):
        """
        A :class:`MappedOrderColumn` representing the same column in the
        reversed order.
        """
        c = copy(self)
        c.oc = c.oc.reversed
        return c

    def __str__(self):
        return str(self.oc)


class DirectColumn(MappedOrderColumn):
    """
    An ordering key that was directly included as a column in the original
    query.
    """

    def __init__(self, oc, index):
        super().__init__(oc)
        self.index = index

    def get_from_row(self, row):
        return row[self.index]

    def __repr__(self):
        return f"Direct({self.index}, {self.oc!r})"


class AttributeColumn(MappedOrderColumn):
    """
    An ordering key that was included as a column attribute in the original
    query.
    """

    def __init__(self, oc, index, attr):
        super().__init__(oc)
        self.index = index
        self.attr = attr

    def get_from_row(self, row):
        return getattr(row[self.index], self.attr)

    def __repr__(self):
        return f"Attribute({self.index}.{self.attr}, {self.oc!r})"


class AppendedColumn(MappedOrderColumn):
    """
    An ordering key that requires an additional column to be added to the
    original query.
    """

    _counter = 0

    def __init__(self, oc, name=None):
        super().__init__(oc)
        if not name:
            AppendedColumn._counter += 1
            name = f"_sqlakeyset_oc_{AppendedColumn._counter}"
        self.name = name
        self.extra_column = self.oc.comparable_value.label(self.name)  # type: ignore

    def get_from_row(self, row):
        return getattr(row, self.name)

    @property
    def ob_clause(self):
        col = self.extra_column
        return col if self.oc.is_ascending else col.desc()  # type: ignore

    def __repr__(self):
        return f"Appended({self.oc!r})"


def derive_order_key(
    ocol: OC, desc: Union[ColumnElement, dict], index: int, strip_label: bool = False
) -> Optional[MappedOrderColumn]:
    """
    Attempt to derive the value of `ocol` from a query column.

    :param ocol: The :class:`OC` to look up.
    :param desc: Either a column description as in
        :attr:`sqlalchemy.orm.query.Query.column_descriptions`, or a
        :class:`sqlalchemy.sql.expression.ColumnElement`.

    :returns: Either a :class:`MappedOrderColumn` or `None`.
    """
    if isinstance(desc, ColumnElement):
        if desc.compare(ocol.comparable_value):
            return DirectColumn(ocol, index)
        else:
            return None

    entity = desc["entity"]
    expr = desc["expr"]

    if isinstance(expr, Bundle):
        for key, col in dict(expr.columns).items():
            if strip_labels(col).compare(ocol.comparable_value):
                return AttributeColumn(ocol, index, key)

    try:
        is_a_table = bool(entity == expr)
    except (ArgumentError, TypeError):
        is_a_table = False

    if isinstance(expr, Mapper) and expr.class_ == entity:
        is_a_table = True

    if is_a_table:  # is a table
        mapper = class_mapper(desc["type"])
        try:
            prop = mapper.get_property_by_column(ocol.element)
            return AttributeColumn(ocol, index, prop.key)
        except UnmappedColumnError:
            pass

    # is an attribute of some kind
    if isinstance(expr, QueryableAttribute):
        # We do our best here, but some attributes (e.g. hybrid properties)
        # are very difficult to identify correctly, so those can fail and
        # result in an AppendedColumn even when present in the selected
        # entities.
        try:
            mapper = expr.parent
            # TODO: is this name-based identification solid?
            # Seems like weird self-joins with aliases or labels could
            # result in false positives here...
            tname = mapper.local_table.description
            if ocol.table_name == tname and ocol.name == expr.name:
                return DirectColumn(ocol, index)
        except AttributeError:
            pass

    # is an attribute with label
    try:
        if ocol.quoted_full_name == OC(expr).full_name:
            return DirectColumn(ocol, index)
    except ArgumentError:
        pass

    if isinstance(expr, Label) and ocol.name == expr.name:
        if f"{expr._element.table.name}.{expr._element.name}" == OC(expr).full_name:
            order_func = _get_order_direction(ocol.uo) or asc
            if strip_label:
                return DirectColumn(OC(order_func(expr.element)), index)
            else:
                return DirectColumn(ocol, index)

    return None


def find_order_key(
    ocol: OC,
    column_descriptions: List[Union[ColumnElement, dict]],
    strip_label: bool = False,
) -> MappedOrderColumn:
    """
    Return a :class:`MappedOrderColumn` describing how to populate the
    ordering column `ocol` from a query returning columns described by
    `column_descriptions`.

    :param ocol: The :class:`OC` to look up.
    :param column_descriptions: The list of columns from which to attempt to
        derive the value of `ocol`.
    :returns: A :class:`MappedOrderColumn` wrapping `ocol`.
    """
    for index, desc in enumerate(column_descriptions):
        ok = derive_order_key(ocol, desc, index, strip_label)
        if ok is not None:
            return ok

    # Couldn't find an existing column in the query from which we can
    # determine this ordering column; so we need to add one.
    return AppendedColumn(ocol)