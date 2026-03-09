import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import React, { useMemo, useState } from "react";

export default function DataTable({
  columns,
  data,
  emptyLabel = "No rows.",
  className = "",
  initialSorting = [],
}) {
  const [sorting, setSorting] = useState(initialSorting);
  const stableColumns = useMemo(() => columns, [columns]);
  const stableData = useMemo(() => data || [], [data]);

  const table = useReactTable({
    data: stableData,
    columns: stableColumns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  if (!stableData.length) {
    return <div className="empty-state">{emptyLabel}</div>;
  }

  return (
    <div className={`data-table-wrap ${className}`.trim()}>
      <table className="data-table">
        <thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => {
                const canSort = header.column.getCanSort();
                const sortState = header.column.getIsSorted();
                return (
                  <th
                    key={header.id}
                    className={canSort ? "data-table-sortable" : ""}
                    onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                  >
                    <span className="data-table-head">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {sortState ? <span className="data-table-sort-indicator">{sortState === "asc" ? "↑" : "↓"}</span> : null}
                    </span>
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
