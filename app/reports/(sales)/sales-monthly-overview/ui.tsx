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

  const columnDefs: (ColDef | ColGroupDef)[] = [
    {
      field: "구분",
      headerName: "구 분",
      width: 100,
      pinned: "left",
      cellStyle: { fontWeight: "bold", backgroundColor: "#f9fafb" },
    },
    {
      headerName: "1Q",
      children: [
        {
          field: "2024.10",
          headerName: "2024.10",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2024.11",
          headerName: "2024.11",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2024.12",
          headerName: "2024.12",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "2Q",
      children: [
        {
          field: "2025.01",
          headerName: "2025.01",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.02",
          headerName: "2025.02",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.03",
          headerName: "2025.03",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "3Q",
      children: [
        {
          field: "2025.04",
          headerName: "2025.04",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.05",
          headerName: "2025.05",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.06",
          headerName: "2025.06",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "4Q",
      children: [
        {
          field: "2025.07",
          headerName: "2025.07",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.08",
          headerName: "2025.08",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
        {
          field: "2025.09",
          headerName: "2025.09",
          width: 90,
          valueFormatter: (params: any) =>
            params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      field: "합계",
      headerName: "합 계",
      width: 100,
      cellStyle: { fontWeight: "bold", backgroundColor: "#e5f3ff" },
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
  ];

  const rowData = [
    {
      구분: "국 영",
      "2024.10": 1363,
      "2024.11": 2001,
      "2024.12": 2879,
      "2025.01": 1992,
      "2025.02": 2199,
      "2025.03": 2466,
      "2025.04": 2767,
      "2025.05": 3001,
      "2025.06": 2668,
      "2025.07": 3759,
      "2025.08": 4218,
      "2025.09": 3689,
      합계: 33000,
    },
    {
      구분: "B2B",
      "2024.10": 0,
      "2024.11": 0,
      "2024.12": 0,
      "2025.01": 0,
      "2025.02": 0,
      "2025.03": 30,
      "2025.04": 0,
      "2025.05": 170,
      "2025.06": 300,
      "2025.07": 500,
      "2025.08": 0,
      "2025.09": 1000,
      합계: 2000,
    },
    {
      구분: "조 달",
      "2024.10": 782,
      "2024.11": 728,
      "2024.12": 1896,
      "2025.01": 1339,
      "2025.02": 1745,
      "2025.03": 1475,
      "2025.04": 873,
      "2025.05": 1044,
      "2025.06": 2166,
      "2025.07": 2517,
      "2025.08": 1711,
      "2025.09": 1726,
      합계: 18000,
    },
    {
      구분: "해 영",
      "2024.10": 442,
      "2024.11": 919,
      "2024.12": 940,
      "2025.01": 926,
      "2025.02": 980,
      "2025.03": 1064,
      "2025.04": 1038,
      "2025.05": 1127,
      "2025.06": 1157,
      "2025.07": 1202,
      "2025.08": 1210,
      "2025.09": 1180,
      합계: 12185,
    },
    {
      구분: "철 도",
      "2024.10": 0,
      "2024.11": 605,
      "2024.12": 0,
      "2025.01": 0,
      "2025.02": 0,
      "2025.03": 0,
      "2025.04": 0,
      "2025.05": 95,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 1,
      "2025.09": 100,
      합계: 800,
    },
    {
      구분: "S I",
      "2024.10": 614,
      "2024.11": 25,
      "2024.12": 165,
      "2025.01": 125,
      "2025.02": 225,
      "2025.03": 489,
      "2025.04": 424,
      "2025.05": 801,
      "2025.06": 125,
      "2025.07": 627,
      "2025.08": 1025,
      "2025.09": 1356,
      합계: 6000,
    },
    {
      구분: "인터렉트",
      "2024.10": 7,
      "2024.11": 10,
      "2024.12": 25,
      "2025.01": 35,
      "2025.02": 43,
      "2025.03": 80,
      "2025.04": 120,
      "2025.05": 120,
      "2025.06": 120,
      "2025.07": 120,
      "2025.08": 120,
      "2025.09": 200,
      합계: 1000,
    },
    {
      구분: "합 계",
      "2024.10": 3208,
      "2024.11": 4287,
      "2024.12": 5905,
      "2025.01": 4417,
      "2025.02": 5192,
      "2025.03": 5604,
      "2025.04": 5221,
      "2025.05": 6358,
      "2025.06": 6534,
      "2025.07": 8724,
      "2025.08": 8285,
      "2025.09": 9250,
      합계: 72985,
    },
    {
      구분: "국 영",
      "2024.10": 1313,
      "2024.11": 1832,
      "2024.12": 2101,
      "2025.01": 1733,
      "2025.02": 1702,
      "2025.03": 2716,
      "2025.04": 2767,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 14165,
    },
    {
      구분: "B2B",
      "2024.10": 0,
      "2024.11": 0,
      "2024.12": 0,
      "2025.01": 0,
      "2025.02": 0,
      "2025.03": 0,
      "2025.04": 0,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 0,
    },
    {
      구분: "조 달",
      "2024.10": 491,
      "2024.11": 396,
      "2024.12": 860,
      "2025.01": 388,
      "2025.02": 988,
      "2025.03": 858,
      "2025.04": 873,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 4854,
    },
    {
      구분: "해 영",
      "2024.10": 491,
      "2024.11": 246,
      "2024.12": 1758,
      "2025.01": 310,
      "2025.02": 532,
      "2025.03": 758,
      "2025.04": 1035,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 5131,
    },
    {
      구분: "철 도",
      "2024.10": 0,
      "2024.11": 598,
      "2024.12": 0,
      "2025.01": 0,
      "2025.02": 9,
      "2025.03": 109,
      "2025.04": 26,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 741,
    },
    {
      구분: "S I",
      "2024.10": 820,
      "2024.11": 516,
      "2024.12": 665,
      "2025.01": 963,
      "2025.02": 247,
      "2025.03": 620,
      "2025.04": 424,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 4254,
    },
    {
      구분: "인터렉트",
      "2024.10": 0,
      "2024.11": 0,
      "2024.12": 37,
      "2025.01": 31,
      "2025.02": 1,
      "2025.03": 66,
      "2025.04": 120,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 255,
    },
    {
      구분: "합 계",
      "2024.10": 3115,
      "2024.11": 3588,
      "2024.12": 5421,
      "2025.01": 3425,
      "2025.02": 3479,
      "2025.03": 5126,
      "2025.04": 5245,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: 29399,
    },
    {
      구분: "차액(목표-실적)",
      "2024.10": -93,
      "2024.11": -700,
      "2024.12": -485,
      "2025.01": -991,
      "2025.02": -1713,
      "2025.03": -478,
      "2025.04": 24,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: -43586,
    },
    {
      구분: "일 달성률(%)",
      "2024.10": "97.1%",
      "2024.11": "83.7%",
      "2024.12": "91.8%",
      "2025.01": "77.6%",
      "2025.02": "67.0%",
      "2025.03": "91.5%",
      "2025.04": "100.5%",
      "2025.05": "0.0%",
      "2025.06": "0.0%",
      "2025.07": "0.0%",
      "2025.08": "0.0%",
      "2025.09": "0.0%",
      합계: "40.3%",
    },
    {
      구분: "목표 누계",
      "2024.10": 3208,
      "2024.11": 7495,
      "2024.12": 13401,
      "2025.01": 17817,
      "2025.02": 23009,
      "2025.03": 28613,
      "2025.04": 0,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: "",
    },
    {
      구분: "실적 누계",
      "2024.10": 3115,
      "2024.11": 6703,
      "2024.12": 12123,
      "2025.01": 15549,
      "2025.02": 19028,
      "2025.03": 24154,
      "2025.04": 29399,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: "",
    },
    {
      구분: "차액(목표-실적)",
      "2024.10": -93,
      "2024.11": -792,
      "2024.12": -1277,
      "2025.01": -2269,
      "2025.02": -3981,
      "2025.03": -4459,
      "2025.04": -4435,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
      합계: "",
    },
    {
      구분: "누적 달성률(%)",
      "2024.10": "97.1%",
      "2024.11": "89.4%",
      "2024.12": "90.5%",
      "2025.01": "87.3%",
      "2025.02": "82.7%",
      "2025.03": "84.4%",
      "2025.04": "86.9%",
      "2025.05": "0.0%",
      "2025.06": "0.0%",
      "2025.07": "0.0%",
      "2025.08": "0.0%",
      "2025.09": "0.0%",
      합계: "0.0%",
    },
  ];

  const getRowStyle = (params: any) => {
    return {};
  };

  const rowClassRules = {
    "section-divider": (params: any) =>
      params.data.구분 === "합 계" && params.node.rowIndex === 7,
  };

  return (
    <div className="min-h-screen">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">월별 목표/실적 개요(Overview)</h1>
          <span className="text-lg font-medium">{selectedPeriod} : 전체</span>
        </div>
        <div className="flex items-center gap-4 mb-4">
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

      <AppAgGrid
        rowData={rowData}
        columnDefs={columnDefs}
        defaultColDef={{
          minWidth: 80,
          resizable: true,
          sortable: false,
          filter: false,
          cellStyle: {
            textAlign: "right",
            fontSize: "12px",
          },
        }}
        getRowStyle={getRowStyle}
        rowClassRules={rowClassRules}
        headerHeight={35}
        rowHeight={35}
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

        .section-divider {
          border-bottom: 2px solid #9ca3af !important;
        }
      `}</style>
    </div>
  );
}
