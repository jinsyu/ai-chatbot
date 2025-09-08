"use client";

import { AgGridReact } from "ag-grid-react";
import { AG_GRID_LOCALE_KR } from "@ag-grid-community/locale";
import type {
  ColDef,
  SizeColumnsToFitGridStrategy,
  SizeColumnsToContentStrategy,
  SizeColumnsToFitProvidedWidthStrategy,
  RowStyle,
} from "ag-grid-community";
import {
  ModuleRegistry,
  AllEnterpriseModule,
  themeBalham,
  LicenseManager,
} from "ag-grid-enterprise";
import { cn } from "@/lib/utils";
import { useEffect, useState } from "react";
import AgGridPagination from "./ag-grid-pagination";

LicenseManager.setLicenseKey(
  "Using_this_{AG_Grid}_Enterprise_key_{AG-090080}_in_excess_of_the_licence_granted_is_not_permitted___Please_report_misuse_to_legal@ag-grid.com___For_help_with_changing_this_key_please_contact_info@ag-grid.com___{BASEONE_Co.,_Ltd.}_is_granted_a_{Multiple_Applications}_Developer_License_for_{1}_Front-End_JavaScript_developer___All_Front-End_JavaScript_developers_need_to_be_licensed_in_addition_to_the_ones_working_with_{AG_Grid}_Enterprise___This_key_has_not_been_granted_a_Deployment_License_Add-on___This_key_works_with_{AG_Grid}_Enterprise_versions_released_before_{17_June_2026}____[v3]_[01]_MTc4MTY1MDgwMDAwMA==5b9d0f9d7c5d50380a34a02611b12147"
);
ModuleRegistry.registerModules([AllEnterpriseModule]);

const DEFAULT_COL_DEF = {
  minWidth: 100,
  resizable: true,
  sortable: true,
  filter: true,
  autoHeight: false,
  floatingFilter: false,
  suppressMovable: false,
  suppressHeaderMenuButton: true,
  cellStyle: {
    display: "flex",
    alignItems: "center",
    fontSize: "13px",
    padding: "0 12px",
  },
  headerClass: "ag-header-cell-custom",
};

export default function AppAgGrid({
  className,
  ref,
  rowData,
  columnDefs,
  handleCellClick,
  actions,
  onGridReady,
  defaultColDef = DEFAULT_COL_DEF,
  onRowClicked,
  autoSizeStrategy = {
    type: "fitGridWidth",
  },
  title = "목록",
  rowStyle = { cursor: "pointer" },
  getRowStyle,
  rowClassRules,
  rowHeight = 45,
  headerHeight = 40,
  animateRows = true,
  showPagination = true,
  paginationPageSize = 20,
  pageSizeOptions = [10, 20, 50, 100],
}: {
  className?: string;
  ref?: React.RefObject<AgGridReact<any>>;
  rowData: any[];
  columnDefs: ColDef[];
  hasDeletePermission?: boolean;
  hasUpdatePermission?: boolean;
  handleCellClick?: (params: any) => void;
  actions?: React.ReactNode[];
  onGridReady?: (params: any) => void;
  defaultColDef?: ColDef;
  onRowClicked?: (params: any) => void;
  autoSizeStrategy?:
    | SizeColumnsToFitGridStrategy
    | SizeColumnsToFitProvidedWidthStrategy
    | SizeColumnsToContentStrategy
    | undefined;
  title?: string;
  rowStyle?: RowStyle;
  getRowStyle?: (params: any) => any;
  rowClassRules?: any;
  rowHeight?: number;
  headerHeight?: number;
  animateRows?: boolean;
  showPagination?: boolean;
  paginationPageSize?: number;
  pageSizeOptions?: number[];
}) {
  const [gridApi, setGridApi] = useState<any>(null);

  // 데이터가 변경될 때 autoSizeStrategy 재적용
  useEffect(() => {
    if (gridApi && rowData && rowData.length > 0) {
      // 데이터 변경 후 약간의 지연을 주어 DOM이 업데이트된 후 실행
      const timer = setTimeout(() => {
        gridApi.updateGridOptions({ autoSizeStrategy });
      }, 100);

      return () => clearTimeout(timer);
    }
  }, [rowData, gridApi, autoSizeStrategy]);

  return (
    <div
      className={cn(
        "flex flex-col gap-2 rounded-xl border-none shadow-none bg-white",
        className
      )}
    >
      <div className="flex justify-between items-center">
        <div className="text-lg font-bold">{title}</div>
        <div className="flex gap-2">
          {actions?.map((action, index) => (
            <div key={index}>{action}</div>
          ))}
        </div>
      </div>
      <AgGridReact
        ref={ref}
        theme={themeBalham}
        defaultColDef={defaultColDef}
        rowData={rowData}
        columnDefs={columnDefs}
        domLayout="autoHeight"
        onCellClicked={handleCellClick}
        onRowClicked={onRowClicked}
        pagination={showPagination}
        paginationPageSize={paginationPageSize}
        suppressPaginationPanel={true}
        overlayNoRowsTemplate="<span>검색 결과가 없습니다.</span>"
        autoSizeStrategy={autoSizeStrategy}
        localeText={AG_GRID_LOCALE_KR}
        rowClassRules={rowClassRules}
        getRowStyle={
          getRowStyle ||
          ((params: any) => {
            if (params.node.rowIndex % 2 === 0) {
              return { backgroundColor: "#f9fafb" };
            }
            return { backgroundColor: "#ffffff" };
          })
        }
        onGridReady={(params: any) => {
          // gridApi 저장
          setGridApi(params.api);

          // 행 높이 설정
          params.api.setGridOption("rowHeight", rowHeight);
          params.api.setGridOption("headerHeight", headerHeight);
          // 애니메이션 활성화
          params.api.setGridOption("animateRows", animateRows);

          // 데이터가 있을 때 초기 컬럼 크기 조정
          if (rowData && rowData.length > 0) {
            // autoSizeStrategy를 재적용하여 flex 컬럼이 제대로 작동하도록 함
            setTimeout(() => {
              params.api.updateGridOptions({ autoSizeStrategy });
            }, 0);
          }

          // 사용자 정의 onGridReady 호출
          onGridReady?.(params);
        }}
        rowStyle={rowStyle}
        rowHeight={rowHeight}
        headerHeight={headerHeight}
        animateRows={animateRows}
      />
      {showPagination && gridApi && (
        <AgGridPagination gridApi={gridApi} pageSizeOptions={pageSizeOptions} />
      )}
    </div>
  );
}
