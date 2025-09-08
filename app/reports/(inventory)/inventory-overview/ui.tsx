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
import type { ColDef } from "ag-grid-community";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  ComposedChart,
} from "recharts";

export default function InventoryOverviewUI() {
  const [selectedPeriod, setSelectedPeriod] = useState("2025.03");

  // KPI 데이터
  const kpiData = [
    { label: "총 재고수량", value: "204,799" },
    { label: "총 재고금액", value: "8,560,136,667" },
    { label: "재고회전율", value: "3.4" },
  ];

  // 월별 재고 추이 차트 데이터
  const monthlyData = [
    { month: "2024.10", 재고금액: 9438, 재고수량: 250 },
    { month: "2024.11", 재고금액: 9146, 재고수량: 240 },
    { month: "2024.12", 재고금액: 8374, 재고수량: 220 },
    { month: "2025.01", 재고금액: 8864, 재고수량: 230 },
    { month: "2025.02", 재고금액: 8990, 재고수량: 235 },
    { month: "2025.03", 재고금액: 8560, 재고수량: 205 },
  ];

  // 파이 차트 데이터
  const pieData = [
    { name: "정상재고", value: 81, color: "#3b82f6" },
    { name: "장기재고", value: 10, color: "#fbbf24" },
    { name: "악성재고", value: 8, color: "#ef4444" },
    { name: "신제품", value: 1, color: "#10b981" },
  ];

  // 테이블 컬럼 정의
  const columnDefs: ColDef[] = [
    { field: "자재번호", headerName: "자재번호", width: 120 },
    { field: "자재명", headerName: "자재명", width: 180 },
    { field: "제품군", headerName: "제품군", width: 100 },
    { field: "자재그룹", headerName: "자재그룹", width: 120 },
    {
      field: "재고상태",
      headerName: "재고상태",
      width: 100,
      cellStyle: (params) => {
        if (params.value === "정상") return { color: "#10b981" };
        if (params.value === "유통") return { color: "#f59e0b" };
        if (params.value === "유통A") return { color: "#f59e0b" };
        return { color: "#ef4444" };
      },
    },
    {
      field: "단종여부",
      headerName: "단종여부",
      width: 100,
    },
    {
      field: "총재고수량",
      headerName: "총 재고수량",
      width: 120,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "가용재고수량",
      headerName: "가용 재고수량",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "자산재고금액",
      headerName: "자산 재고금액",
      width: 140,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "자갈재고금액",
      headerName: "자갈 재고금액",
      width: 140,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "최근입고일",
      headerName: "최근입고일",
      width: 120,
    },
    {
      field: "최근출고일",
      headerName: "최근출고일",
      width: 120,
    },
    {
      field: "월평균판매량",
      headerName: "월평균판매량",
      width: 120,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "재고소진예상기간",
      headerName: "재고소진예상기간",
      width: 140,
    },
    {
      field: "매출등급",
      headerName: "매출등급",
      width: 100,
      cellStyle: (params) => {
        if (params.value === "A") return { backgroundColor: "#dcfce7" };
        if (params.value === "B") return { backgroundColor: "#fef3c7" };
        if (params.value === "C") return { backgroundColor: "#fee2e2" };
        return null;
      },
    },
  ];

  // 테이블 데이터
  const rowData = [
    {
      자재번호: "302134",
      자재명: "PD-6359 시판",
      제품군: "PA",
      자재그룹: "6000 SYSTEM",
      재고상태: "정상",
      단종여부: "",
      총재고수량: 724,
      가용재고수량: 724,
      자산재고금액: 239339196,
      자갈재고금액: 239339196,
      최근입고일: "2025-04-07",
      최근출고일: "2025-04-09",
      월평균판매량: 91,
      재고소진예상기간: "8.0",
      매출등급: "A",
    },
    {
      자재번호: "471069",
      자재명: "CS-610 시판",
      제품군: "PA",
      자재그룹: "CEILING SPK",
      재고상태: "정상",
      단종여부: "",
      총재고수량: 14222,
      가용재고수량: 14186,
      자산재고금액: 184658448,
      자갈재고금액: 184191024,
      최근입고일: "2025-04-07",
      최근출고일: "2025-04-09",
      월평균판매량: 8709,
      재고소진예상기간: "1.6",
      매출등급: "A",
    },
    {
      자재번호: "306283",
      자재명: "PFM-M250 조달",
      제품군: "AV",
      자재그룹: "A/V SOLUTION",
      재고상태: "신제품",
      단종여부: "",
      총재고수량: 84,
      가용재고수량: 81,
      자산재고금액: 127330812,
      자갈재고금액: 122783283,
      최근입고일: "2024-05-16",
      최근출고일: "2025-03-31",
      월평균판매량: 2,
      재고소진예상기간: "40.5",
      매출등급: "B",
    },
    {
      자재번호: "303746",
      자재명: "WG-A91L 시판",
      제품군: "PA",
      자재그룹: "Wireless MIC",
      재고상태: "장기재고",
      단종여부: "04",
      총재고수량: 501,
      가용재고수량: 500,
      자산재고금액: 70370460,
      자갈재고금액: 70230000,
      최근입고일: "2021-06-23",
      최근출고일: "2025-03-25",
      월평균판매량: 0,
      재고소진예상기간: "신출불가",
      매출등급: "C",
    },
    {
      자재번호: "303708",
      자재명: "PAC-5600IRQ R군",
      제품군: "PA",
      자재그룹: "COMBI AMP",
      재고상태: "악성재고",
      단종여부: "04",
      총재고수량: 15,
      가용재고수량: 15,
      자산재고금액: 21811590,
      자갈재고금액: 21811590,
      최근입고일: "2020-08-19",
      최근출고일: "2020-08-25",
      월평균판매량: 0,
      재고소진예상기간: "신출불가",
      매출등급: "C",
    },
  ];

  return (
    <div className="flex flex-col gap-6">
      {/* 헤더 */}
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">재고 개요(Overview)</h1>
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
          <Select>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="공급엄체" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025.03">2025.03</SelectItem>
              <SelectItem value="2025.02">2025.02</SelectItem>
              <SelectItem value="2025.01">2025.01</SelectItem>
            </SelectContent>
          </Select>
          <Select>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="부서명" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025.03">2025.03</SelectItem>
              <SelectItem value="2025.02">2025.02</SelectItem>
              <SelectItem value="2025.01">2025.01</SelectItem>
            </SelectContent>
          </Select>

          <Select>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="자재그룹" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025.03">2025.03</SelectItem>
              <SelectItem value="2025.02">2025.02</SelectItem>
              <SelectItem value="2025.01">2025.01</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* KPI 카드 */}
      <div className="grid grid-cols-3 gap-4">
        {kpiData.map((kpi) => (
          <Card key={kpi.label} className="border-gray-200">
            <CardContent className="p-6">
              <div className="text-sm text-gray-600 mb-2">{kpi.label}</div>
              <div className="text-3xl font-bold text-gray-900">
                {kpi.value}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* 차트 섹션 */}
      <div className="grid grid-cols-3 gap-4">
        {/* 월별 재고 추이 */}
        <Card className="border-gray-200 col-span-2">
          <CardHeader>
            <CardTitle className="text-base font-medium">
              월별 재고 추이
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={monthlyData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" />
                <YAxis yAxisId="left" orientation="left" />
                <YAxis yAxisId="right" orientation="right" />
                <Tooltip />
                <Legend />
                <Bar
                  yAxisId="left"
                  dataKey="재고금액"
                  fill="#6366f1"
                  barSize={50}
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="재고수량"
                  stroke="#ef4444"
                  strokeWidth={2}
                  dot={{ fill: "#ef4444", r: 4 }}
                  activeDot={{ r: 6 }}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* 재고상태별 재고분포 */}
        <Card className="border-gray-200">
          <CardHeader>
            <CardTitle className="text-base font-medium">
              재고상태별 재고분포
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={250}>
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={(entry) => `${entry.value}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {pieData.map((entry) => (
                    <Cell key={entry.name} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      {/* 재고 상세 테이블 */}
      <Card className="border-gray-200">
        <CardHeader>
          <CardTitle className="text-base font-medium">
            재고 상세 테이블
          </CardTitle>
          <div className="text-sm text-gray-500 mt-1">검색</div>
        </CardHeader>
        <CardContent>
          <AppAgGrid
            rowData={rowData}
            columnDefs={columnDefs}
            defaultColDef={{
              resizable: true,
              sortable: true,
              filter: true,
            }}
            title=""
            showPagination={true}
          />
        </CardContent>
      </Card>
    </div>
  );
}
