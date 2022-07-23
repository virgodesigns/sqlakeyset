"""
Main paging interface.

The modules in this directory are a heavily modified version of the "sqlakeyset" library, see:
1. https://github.com/djrobstep/sqlakeyset

We started by making the library compatible with `asyncio` and 2.0 SQLAlchemy style, and ended up only keeping the
parts we need.
"""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

from sqlalchemy import tuple_
from sqlalchemy.sql.selectable import CompoundSelect, Select

from aio_sqlakeyset.columns import OC, find_order_key, parse_ob_clause
from aio_sqlakeyset.results import Page, Paging
from aio_sqlakeyset.serial import InvalidPage

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from aio_sqlakeyset.columns import MappedOrderColumn


@dataclass
class TransformationResponse:
    selectable: Union[Select, CompoundSelect]
    mapped_ocols: List["MappedOrderColumn"]
    extra_columns: List[Any]


def where_condition_for_page(
    ordering_columns: List[OC], place: Tuple[Any], db: "AsyncSession"
):
    """
    Construct the SQL condition required to restrict a selectable to the desired page.

    :param ordering_columns: The query's ordering columns
    :param place: The starting position for the page
    :returns: An SQLAlchemy expression suitable for use in `.filter` or `.having`.

    Raises:
        InvalidPage: If `place` does not correspond to the given OCs.
    """
    if len(ordering_columns) != len(place):
        raise InvalidPage(
            "Page marker has different column count to query's order clause"
        )

    dialect = db.bind.dialect
    zipped = zip(ordering_columns, place)
    swapped = [c.pair_for_comparison(value, dialect) for c, value in zipped]
    row, place_row = zip(*swapped)

    if len(row) == 1:
        condition = row[0] > place_row[0]  # type: ignore
    else:
        condition = tuple_(*row) > tuple_(*place_row)  # type: ignore
    return condition


def _add_keyset_condition(
    selectable: Select, order_cols: List[OC], db: "AsyncSession", place: Tuple[Any]
) -> Select:
    dereferenced_order_cols = [
        find_order_key(ocol, selectable.column_descriptions, strip_label=True).oc
        for ocol in order_cols
    ]
    condition = where_condition_for_page(dereferenced_order_cols, place, db)
    # If there is at least one GROUP BY clause, we have an aggregate query.
    # In this case, the paging condition is applied AFTER aggregation. To do so, we must use HAVING and not FILTER.
    if selectable._group_by_clauses:  # type: ignore
        select = selectable.having(condition)
    else:
        select = selectable.where(condition)
    return select


def _transform_compound_selectable(
    selectable: CompoundSelect,
    order_cols: List[OC],
    db: "AsyncSession",
    place: Optional[Tuple[Any]] = None,
) -> TransformationResponse:
    column_descriptions = []
    select_stmt: "Select"
    for select_stmt in selectable.selects:
        column_descriptions += select_stmt.column_descriptions

    # Get the actual columns from the ordering columns
    mapped_ocols = [find_order_key(ocol, column_descriptions) for ocol in order_cols]

    new_order_by_clauses = [col.ob_clause for col in mapped_ocols]
    selectable_with_order_by = selectable.order_by(None).order_by(*new_order_by_clauses)

    # If there are any columns in order_by clause but not in select, add them
    extra_columns = [
        col.extra_column for col in mapped_ocols if col.extra_column is not None
    ]
    # We have to add extra column to each of the select statements in compound select.
    for i, select_stmt in enumerate(selectable_with_order_by.selects):
        select_stmt.add_columns(*extra_columns)
        selectable_with_order_by.selects[i] = select_stmt

    if place:
        # Prepare the condition for selecting a specific page.
        for i, select_stmt in enumerate(selectable_with_order_by.selects):
            selectable_with_order_by.selects[i] = _add_keyset_condition(
                select_stmt, order_cols, db, place
            )

    return TransformationResponse(
        selectable=selectable_with_order_by,
        mapped_ocols=mapped_ocols,
        extra_columns=extra_columns,
    )


def _transform_selectable(
    selectable: Select,
    order_cols: List[OC],
    db: "AsyncSession",
    place: Optional[Tuple[Any]] = None,
) -> TransformationResponse:
    mapped_ocols = [
        find_order_key(ocol, selectable.column_descriptions) for ocol in order_cols
    ]
    # Update the selectable with the new order_by clauses.
    new_order_by_clauses = [col.ob_clause for col in mapped_ocols]
    selectable_with_order_by = selectable.order_by(None).order_by(*new_order_by_clauses)

    # Add the extra columns required for the ordering.
    extra_columns = [
        col.extra_column for col in mapped_ocols if col.extra_column is not None
    ]

    selectable_with_extra_columns = selectable_with_order_by.add_columns(*extra_columns)
    selectable_final = selectable_with_extra_columns

    if place:
        selectable_final = _add_keyset_condition(
            selectable_with_extra_columns, order_cols, db, place
        )

    return TransformationResponse(
        selectable=selectable_final,
        mapped_ocols=mapped_ocols,
        extra_columns=extra_columns,
    )


async def get_page(
    selectable: Union[Select, CompoundSelect],
    per_page: int,
    db: "AsyncSession",
    place: Optional[Tuple[Any]] = None,
    backwards: bool = False,
) -> Page:
    """
    Get a page from an SQLAlchemy Core selectable.

    Args:
        selectable: The source selectable.
        per_page: Number of rows per page.
        place: Keyset representing the place after which to start the page.
        backwards: If ``True``, reverse pagination direction.

    Returns:
        The result page.
    """
    # Build a list of ordering columns (ocols) in the form of `MappedOrderColumn` objects.
    order_cols = parse_ob_clause(selectable, backwards)
    if isinstance(selectable, CompoundSelect):
        transformed = _transform_compound_selectable(selectable, order_cols, db, place)
    else:
        transformed = _transform_selectable(selectable, order_cols, db, place)

    # Limit the amount of results in the page. The 1 extra is to check if there's a further page.
    selectable = transformed.selectable
    selectable = selectable.limit(per_page + 1)

    # Run the selectable and get back the query rows.
    # NOTE: Do not use `.scalars` here, as it might lead to some rows being omitted by the ORM.
    selected = await db.execute(selectable)
    row_keys = list(selected.keys())
    rows = selected.all()

    # Finally, construct the `Page` object.
    # Trim off the extra columns and return as a correct-as-possible sqlalchemy Row.
    out_rows: List[tuple] = [
        row[: -len(transformed.extra_columns) or None] for row in rows
    ]
    key_rows = [
        tuple(col.get_from_row(row) for col in transformed.mapped_ocols) for row in rows
    ]
    paging = Paging(out_rows, per_page, order_cols, backwards, place, markers=key_rows)
    return Page(
        paging.rows, paging, keys=row_keys[: -len(transformed.extra_columns) or None]
    )
