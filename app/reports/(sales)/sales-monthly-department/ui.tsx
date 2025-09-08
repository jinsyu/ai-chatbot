"use client";

import { useState } from "react";
import AppAgGrid from "@/components/app-aggrid";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { ColDef, ColGroupDef } from "ag-grid-community";

export default function MonthlyReportUI() {
  const [selectedPeriod, setSelectedPeriod] = useState("42차");
  const [selectedDepartment, setSelectedDepartment] = useState("부서명");

  const columnDefs: (ColDef | ColGroupDef)[] = [
    {
      field: "구분",
      headerName: "구분",
      width: 100,
      pinned: "left",
      cellStyle: {
        fontWeight: "bold",
        // backgroundColor: "#1f2937",
        // color: "white",
      },
    },
    {
      field: "24년 10월",
      headerName: "24년 10월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "24년 11월",
      headerName: "24년 11월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "24년 12월",
      headerName: "24년 12월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 1월",
      headerName: "25년 1월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 2월",
      headerName: "25년 2월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 3월",
      headerName: "25년 3월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 4월",
      headerName: "25년 4월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 5월",
      headerName: "25년 5월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 6월",
      headerName: "25년 6월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 7월",
      headerName: "25년 7월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 8월",
      headerName: "25년 8월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "25년 9월",
      headerName: "25년 9월",
      width: 90,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "100.0%" &&
          params.value
        ) {
          return { color: "#ef4444", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "합계",
      headerName: "합계",
      width: 100,
      valueFormatter: (params: any) => {
        if (params.data.구분?.includes("달성률")) {
          return params.value || "";
        }
        return params.value?.toLocaleString() || "0";
      },
      cellStyle: (params: any) => {
        const baseStyle = { fontWeight: "bold", backgroundColor: "#e5f3ff" };
        if (
          params.data.구분?.includes("달성률") &&
          params.value !== "0.0%" &&
          params.value
        ) {
          return { ...baseStyle, color: "#ef4444" };
        }
        return baseStyle;
      },
    },
  ];

  const rowData = [
    {
      구분: "전년도",
      "24년 10월": 1402,
      "24년 11월": 1701,
      "24년 12월": 3052,
      "25년 1월": 1633,
      "25년 2월": 2100,
      "25년 3월": 1950,
      "25년 4월": 1920,
      "25년 5월": 1806,
      "25년 6월": 3911,
      "25년 7월": 2807,
      "25년 8월": 2905,
      "25년 9월": 5641,
      합계: 30827,
    },
    {
      구분: "목표",
      "24년 10월": 1363,
      "24년 11월": 2001,
      "24년 12월": 2879,
      "25년 1월": 1992,
      "25년 2월": 2199,
      "25년 3월": 2466,
      "25년 4월": 2767,
      "25년 5월": 3001,
      "25년 6월": 2668,
      "25년 7월": 3759,
      "25년 8월": 4218,
      "25년 9월": 3689,
      합계: 33000,
    },
    {
      구분: "실적",
      "24년 10월": 1313,
      "24년 11월": 1832,
      "24년 12월": 2101,
      "25년 1월": 1733,
      "25년 2월": 1702,
      "25년 3월": 2716,
      "25년 4월": 2767,
      "25년 5월": 0,
      "25년 6월": 0,
      "25년 7월": 0,
      "25년 8월": 0,
      "25년 9월": 0,
      합계: 14163,
    },
    {
      구분: "차이",
      "24년 10월": -50,
      "24년 11월": -168,
      "24년 12월": -778,
      "25년 1월": -259,
      "25년 2월": -497,
      "25년 3월": 250,
      "25년 4월": 0,
      "25년 5월": 0,
      "25년 6월": 0,
      "25년 7월": 0,
      "25년 8월": 0,
      "25년 9월": 0,
      합계: -18837,
    },
    {
      구분: "달성률%",
      "24년 10월": "96.3%",
      "24년 11월": "91.6%",
      "24년 12월": "73.0%",
      "25년 1월": "87.0%",
      "25년 2월": "77.4%",
      "25년 3월": "110.1%",
      "25년 4월": "100.0%",
      "25년 5월": "0.0%",
      "25년 6월": "0.0%",
      "25년 7월": "0.0%",
      "25년 8월": "0.0%",
      "25년 9월": "0.0%",
      합계: "42.9%",
    },
    {
      구분: "목표누계",
      "24년 10월": 1363,
      "24년 11월": 3363,
      "24년 12월": 6242,
      "25년 1월": 8234,
      "25년 2월": 10433,
      "25년 3월": 12899,
      "25년 4월": 15666,
      "25년 5월": 0,
      "25년 6월": 0,
      "25년 7월": 0,
      "25년 8월": 0,
      "25년 9월": 0,
      합계: 0,
    },
    {
      구분: "실적누계",
      "24년 10월": 1313,
      "24년 11월": 3145,
      "24년 12월": 5246,
      "25년 1월": 6979,
      "25년 2월": 8681,
      "25년 3월": 11396,
      "25년 4월": 14163,
      "25년 5월": 0,
      "25년 6월": 0,
      "25년 7월": 0,
      "25년 8월": 0,
      "25년 9월": 0,
      합계: 0,
    },
    {
      구분: "차이",
      "24년 10월": -50,
      "24년 11월": -218,
      "24년 12월": -996,
      "25년 1월": -1255,
      "25년 2월": -1752,
      "25년 3월": -1503,
      "25년 4월": -1503,
      "25년 5월": 0,
      "25년 6월": 0,
      "25년 7월": 0,
      "25년 8월": 0,
      "25년 9월": 0,
      합계: 0,
    },
    {
      구분: "달성률%",
      "24년 10월": "96.3%",
      "24년 11월": "93.5%",
      "24년 12월": "84.0%",
      "25년 1월": "84.8%",
      "25년 2월": "83.2%",
      "25년 3월": "88.4%",
      "25년 4월": "90.4%",
      "25년 5월": "0.0%",
      "25년 6월": "0.0%",
      "25년 7월": "0.0%",
      "25년 8월": "0.0%",
      "25년 9월": "0.0%",
      합계: "0.0%",
    },
  ];

  const getRowStyle = (params: any) => {
    if (params.data.구분 === "목표누계" || params.data.구분 === "실적누계") {
      return { backgroundColor: "#f9fafb" };
    }
    return {};
  };

  const rowClassRules = {
    "section-divider": (params: any) =>
      params.data.구분 === "달성률%" && params.node.rowIndex === 4,
  };

  return (
    <div className="min-h-screen">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">부서별 월별 목표/실적</h1>
          <span className="text-lg font-medium">
            {selectedPeriod} : 국내영업팀
          </span>
        </div>
        <div className="flex items-center gap-4 mb-4">
          <div className="flex items-center gap-2">
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="부서명" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">국내영업팀</SelectItem>
                <SelectItem value="41차">조달팀</SelectItem>
                <SelectItem value="40차">해외영업팀</SelectItem>
                <SelectItem value="39차">철도사업팀</SelectItem>
                <SelectItem value="39차">SI팀</SelectItem>
                <SelectItem value="39차">인터랙트팀</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="기준 차수" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">기준 차수</SelectItem>
                <SelectItem value="41차">41차</SelectItem>
                <SelectItem value="40차">40차</SelectItem>
                <SelectItem value="39차">39차</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      <AppAgGrid
        rowData={rowData}
        columnDefs={columnDefs}
        defaultColDef={{
          minWidth: 80,
          resizable: true,
          sortable: false,
          filter: false,
          suppressHeaderMenuButton: true,
          suppressMovable: false,
          cellStyle: {
            textAlign: "right",
          },
        }}
        getRowStyle={getRowStyle}
        rowClassRules={rowClassRules}
        showPagination={false}
        title=""
        autoSizeStrategy={{
          type: "fitGridWidth",
        }}
      />

      <style jsx global>{`
        .ag-header-cell-custom {
          background-color: #374151 !important;
          color: white !important;
          font-weight: bold;
        }
        .ag-header-group-cell {
          background-color: #374151 !important;
          color: white !important;
          font-weight: bold;
        }
        .section-divider {
          border-bottom: 2px solid #9ca3af !important;
        }
      `}</style>
    </div>
  );
}
