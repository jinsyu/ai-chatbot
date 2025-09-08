"use client";

import { useState } from "react";
import AppAgGrid from "@/components/app-aggrid";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
} from "recharts";
import type { ColDef, ColGroupDef } from "ag-grid-community";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";

export default function InventoryMonthlyUI() {
  const [isTargetView, setIsTargetView] = useState(true);
  const [isActualView, setIsActualView] = useState(true);

  // 전년 대비 실적 데이터
  const yearlyComparisonData = [
    { year: "2024.02", value: 8913 },
    { year: "2025.02", value: 8990 },
  ];

  // 월별 재고 목표 대비 실적 차트 데이터
  const monthlyChartData = [
    { month: "2024.10", 목표: 9468, 실적: 9372 },
    { month: "2024.11", 목표: 9146, 실적: 8232 },
    { month: "2024.12", 목표: 8374, 실적: 8358 },
    { month: "2025.01", 목표: 8864, 실적: 8615 },
    { month: "2025.02", 목표: 8990, 실적: 9094 },
    { month: "2025.03", 목표: 8996, 실적: 8996 },
    { month: "2025.04", 목표: 9130, 실적: 9130 },
    { month: "2025.05", 목표: 9044, 실적: 9044 },
    { month: "2025.06", 목표: 9457, 실적: 9457 },
    { month: "2025.07", 목표: 9268, 실적: 9268 },
    { month: "2025.08", 목표: 8385, 실적: 8385 },
    { month: "2025.09", 목표: 7503, 실적: 7503 },
  ];

  // 월별 재고 목표 대비 실적 테이블 데이터
  const monthlyTableData = [
    {
      category: "목표",
      "2024.10": 9468,
      "2024.11": 9372,
      "2024.12": 8232,
      "2025.01": 8358,
      "2025.02": 8615,
      "2025.03": 9094,
      "2025.04": 8996,
      "2025.05": 9130,
      "2025.06": 9044,
      "2025.07": 9457,
      "2025.08": 9268,
      "2025.09": 8385,
    },
    {
      category: "실적",
      "2024.10": 9438,
      "2024.11": 9146,
      "2024.12": 8374,
      "2025.01": 8864,
      "2025.02": 8990,
      "2025.03": 8996,
      "2025.04": 9130,
      "2025.05": 9044,
      "2025.06": 9457,
      "2025.07": 9268,
      "2025.08": 8385,
      "2025.09": 7503,
    },
    {
      category: "차이",
      "2024.10": 66,
      "2024.11": 914,
      "2024.12": 16,
      "2025.01": 249,
      "2025.02": -104,
      "2025.03": 0,
      "2025.04": 0,
      "2025.05": 0,
      "2025.06": 0,
      "2025.07": 0,
      "2025.08": 0,
      "2025.09": 0,
    },
    {
      category: "달성률",
      "2024.10": "99.3%",
      "2024.11": "88.9%",
      "2024.12": "99.8%",
      "2025.01": "97.1%",
      "2025.02": "101.1%",
      "2025.03": "100%",
      "2025.04": "100%",
      "2025.05": "100%",
      "2025.06": "100%",
      "2025.07": "100%",
      "2025.08": "100%",
      "2025.09": "100%",
    },
  ];

  // 제품군 재고 실적 테이블 컬럼 정의
  const productColumnDefs: (ColDef | ColGroupDef)[] = [
    {
      field: "구분",
      headerName: "구분",
      width: 100,
      pinned: "left",
    },
    {
      headerName: "2025.01",
      children: [
        {
          field: "재고수량_01",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
        {
          field: "재고금액_01",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "2025.02",
      children: [
        {
          field: "재고수량_02",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
        {
          field: "재고금액_02",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "전월대비",
      children: [
        {
          field: "재고수량_diff",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
          cellStyle: (params) =>
            params.value < 0 ? { color: "#10b981" } : { color: "#ef4444" },
        },
        {
          field: "재고금액_diff",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
          cellStyle: (params) =>
            params.value < 0 ? { color: "#10b981" } : { color: "#ef4444" },
        },
      ],
    },
  ];

  // 제품군별 재고 실적 데이터
  const productRowData = [
    {
      구분: "제품",
      재고수량_01: 59031,
      재고금액_01: 6232223088,
      재고수량_02: 64228,
      재고금액_02: 6615481908,
      재고수량_diff: 5197,
      재고금액_diff: 383258820,
    },
    {
      구분: "SETS",
      재고수량_01: 15248,
      재고금액_01: 4397120716,
      재고수량_02: 16123,
      재고금액_02: 4775058248,
      재고수량_diff: 875,
      재고금액_diff: 377937532,
    },
    {
      구분: "SPK",
      재고수량_01: 39342,
      재고금액_01: 1492951113,
      재고수량_02: 43907,
      재고금액_02: 1508007779,
      재고수량_diff: 4565,
      재고금액_diff: 15056666,
    },
    {
      구분: "ETC",
      재고수량_01: 4441,
      재고금액_01: 342151259,
      재고수량_02: 4198,
      재고금액_02: 332415881,
      재고수량_diff: -243,
      재고금액_diff: -9735378,
    },
    {
      구분: "상품",
      재고수량_01: 210930,
      재고금액_01: 2631743802,
      재고수량_02: 171341,
      재고금액_02: 2374451738,
      재고수량_diff: -39589,
      재고금액_diff: -257292064,
    },
    {
      구분: "SETS",
      재고수량_01: 2934,
      재고금액_01: 1084895549,
      재고수량_02: 2685,
      재고금액_02: 1024592529,
      재고수량_diff: -249,
      재고금액_diff: -60303020,
    },
    {
      구분: "SPK",
      재고수량_01: 198598,
      재고금액_01: 1209421213,
      재고수량_02: 157946,
      재고금액_02: 997889124,
      재고수량_diff: -40652,
      재고금액_diff: -211532089,
    },
    {
      구분: "ETC",
      재고수량_01: 9398,
      재고금액_01: 337427040,
      재고수량_02: 10710,
      재고금액_02: 351970085,
      재고수량_diff: 1312,
      재고금액_diff: 14543045,
    },
    {
      구분: "합계",
      재고수량_01: 269961,
      재고금액_01: 8863966890,
      재고수량_02: 235569,
      재고금액_02: 8989933646,
      재고수량_diff: -34392,
      재고금액_diff: 125966756,
    },
  ];

  // 부서별 재고 실적 테이블 컬럼 정의
  const deptColumnDefs: (ColDef | ColGroupDef)[] = [
    { field: "구분", headerName: "구분", width: 100, pinned: "left" },
    {
      headerName: "2025.01",
      children: [
        {
          field: "재고수량_01",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
        {
          field: "재고금액_01",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "2025.02",
      children: [
        {
          field: "재고수량_02",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
        {
          field: "재고금액_02",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
        },
      ],
    },
    {
      headerName: "전월대비",
      children: [
        {
          field: "재고수량_diff",
          headerName: "재고수량",
          width: 100,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
          cellStyle: (params) =>
            params.value < 0 ? { color: "#ef4444" } : { color: "#10b981" },
        },
        {
          field: "재고금액_diff",
          headerName: "재고금액",
          width: 120,
          valueFormatter: (params) => params.value?.toLocaleString() || "0",
          cellStyle: (params) =>
            params.value < 0 ? { color: "#ef4444" } : { color: "#10b981" },
        },
      ],
    },
  ];

  // 부서별 재고 실적 데이터
  const deptRowData = [
    {
      구분: "유통",
      재고수량_01: 248901,
      재고금액_01: 5793776979,
      재고수량_02: 207475,
      재고금액_02: 5797523161,
      재고수량_diff: -41426,
      재고금액_diff: 3746182,
    },
    {
      구분: "조달",
      재고수량_01: 8557,
      재고금액_01: 1951377020,
      재고수량_02: 8722,
      재고금액_02: 2008461427,
      재고수량_diff: 165,
      재고금액_diff: 57084407,
    },
    {
      구분: "수출",
      재고수량_01: 12097,
      재고금액_01: 930566644,
      재고수량_02: 18965,
      재고금액_02: 973502093,
      재고수량_diff: 6868,
      재고금액_diff: 42935449,
    },
    {
      구분: "철도",
      재고수량_01: 259,
      재고금액_01: 161633988,
      재고수량_02: 264,
      재고금액_02: 184613078,
      재고수량_diff: 5,
      재고금액_diff: 22979090,
    },
    {
      구분: "인터랙트",
      재고수량_01: 147,
      재고금액_01: 26612259,
      재고수량_02: 143,
      재고금액_02: 25833887,
      재고수량_diff: -4,
      재고금액_diff: -778372,
    },
    {
      구분: "합계",
      재고수량_01: 269961,
      재고금액_01: 8863966890,
      재고수량_02: 235569,
      재고금액_02: 8989933646,
      재고수량_diff: -34392,
      재고금액_diff: 125966756,
    },
  ];

  return (
    <div className="flex flex-col gap-6">
      {/* 헤더 섹션 */}
      <div className="flex items-center justify-between">
        <h1 className="flex items-center gap-4">
          <span className="text-2xl font-bold ">월간 재고 목표/실적</span>
          <span className="text-gray-500">42차 : 2025.03</span>
        </h1>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <Select>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="기준년월" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="2025.03">2025.03</SelectItem>
                <SelectItem value="2025.02">2025.02</SelectItem>
                <SelectItem value="2025.01">2025.01</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* 상단 차트 섹션 */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
        {/* 전년 대비 실적 */}
        <Card className="border-gray-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              전년 대비 실적
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <ResponsiveContainer width="100%" height={150}>
              <BarChart data={yearlyComparisonData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="year" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip />
                <Bar dataKey="value">
                  {yearlyComparisonData.map((entry) => (
                    <Cell
                      key={entry.year}
                      fill={entry.year === "2024.02" ? "#9ca3af" : "#3b82f6"}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* 월별 재고 목표 대비 실적 차트 */}
        <Card className="border-gray-200 lg:col-span-3">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              월별 재고 목표 대비 실적
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <ResponsiveContainer width="100%" height={150}>
              <BarChart data={monthlyChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip />
                <Legend wrapperStyle={{ fontSize: "12px" }} iconType="rect" />
                {isTargetView && <Bar dataKey="목표" fill="#93c5fd" />}
                {isActualView && <Bar dataKey="실적" fill="#6366f1" />}
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      {/* 월별 재고 목표 대비 실적 테이블 */}
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-100">
            <th className="text-left p-2 border">구분</th>
            <th className="text-center p-2 border">41차 기말</th>
            {[
              "2024.10",
              "2024.11",
              "2024.12",
              "2025.01",
              "2025.02",
              "2025.03",
              "2025.04",
              "2025.05",
              "2025.06",
              "2025.07",
              "2025.08",
              "2025.09",
            ].map((month) => (
              <th key={month} className="text-center p-2 border">
                {month}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {monthlyTableData.map((row) => (
            <tr
              key={row.category}
              className={row.category === "달성률" ? "font-semibold" : ""}
            >
              <td className="p-2 border bg-gray-50 font-medium">
                {row.category}
              </td>
              <td className="text-right p-2 border">
                {row.category === "목표"
                  ? "9,468"
                  : row.category === "실적"
                  ? ""
                  : row.category === "차이"
                  ? "66"
                  : "99.3%"}
              </td>
              {Object.entries(row)
                .filter(([key]) => key !== "category")
                .map(([key, value]) => (
                  <td
                    key={key}
                    className={`text-right p-2 border ${
                      row.category === "달성률" && value === "101.1%"
                        ? "bg-blue-100"
                        : row.category === "차이" &&
                          typeof value === "number" &&
                          value < 0
                        ? "text-red-500"
                        : ""
                    }`}
                  >
                    {typeof value === "number" ? value.toLocaleString() : value}
                  </td>
                ))}
            </tr>
          ))}
        </tbody>
      </table>

      {/* 하단 테이블 섹션 */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        {/* 제품군별 재고 실적 */}
        <Card className="border-gray-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              제상품 재고 실적 (전월 비교)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <AppAgGrid
              rowData={productRowData}
              columnDefs={productColumnDefs}
              defaultColDef={{
                resizable: true,
                sortable: false,
              }}
              headerHeight={40}
              rowHeight={32}
              title=""
              showPagination={false}
              getRowStyle={(params) => {
                if (params.data?.구분 === "제품") {
                  return { backgroundColor: "#e0f2fe", fontWeight: "bold" };
                }
                if (params.data?.구분 === "상품") {
                  return { backgroundColor: "#fef3c7", fontWeight: "bold" };
                }
                if (params.data?.구분 === "합계") {
                  return { backgroundColor: "#f3f4f6", fontWeight: "bold" };
                }
                return null;
              }}
            />
          </CardContent>
        </Card>

        {/* 부서별 재고 실적 */}
        <Card className="border-gray-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">
              부서별 재고 실적 (전월 비교)
            </CardTitle>
          </CardHeader>
          <CardContent>
            <AppAgGrid
              rowData={deptRowData}
              columnDefs={deptColumnDefs}
              defaultColDef={{
                resizable: true,
                sortable: false,
              }}
              headerHeight={40}
              rowHeight={48}
              title=""
              showPagination={false}
              getRowStyle={(params) => {
                if (params.data?.구분 === "합계") {
                  return { backgroundColor: "#f3f4f6", fontWeight: "bold" };
                }
                return null;
              }}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
