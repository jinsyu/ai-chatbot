"use client";

import { useState } from "react";
import AppAgGrid from "@/components/app-aggrid";
import type { ColDef } from "ag-grid-community";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from "@/components/ui/select";

export default function InventoryProductGroupUI() {
  const [selectedPeriod, setSelectedPeriod] = useState("2025.02");

  // 테이블 컬럼 정의
  const columnDefs: ColDef[] = [
    {
      field: "자재그룹명",
      headerName: "자재그룹명",
      width: 180,
      pinned: "left",
      cellStyle: (params) => {
        if (params.value === "총 합계") {
          return { backgroundColor: "#e5e7eb", fontWeight: "bold" };
        }
        return null;
      },
    },
    {
      field: "2024-09",
      headerName: "2024-09",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "2024-10",
      headerName: "2024.10",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "2024-11",
      headerName: "2024.11",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "2024-12",
      headerName: "2024.12",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "2025-01",
      headerName: "2025.01",
      width: 130,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "2025-02",
      headerName: "2025.02 (당월)",
      width: 150,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
      cellStyle: { backgroundColor: "#dbeafe", fontWeight: "bold" },
    },
    {
      field: "재고비중",
      headerName: "재고비중",
      width: 100,
      valueFormatter: (params) => params.value || "0%",
    },
    {
      field: "증감율",
      headerName: "증감율(전월)",
      width: 120,
      valueFormatter: (params) => {
        if (params.value) {
          const sign = params.value.startsWith("+") ? "" : "";
          return `${sign}${params.value}`;
        }
        return "0%";
      },
      cellStyle: (params) => {
        if (params.value) {
          const numValue = parseFloat(params.value.replace("%", ""));
          if (numValue < 0) {
            return { color: "#10b981" }; // 재고 감소는 좋은 것
          } else if (numValue > 0) {
            return { color: "#ef4444" }; // 재고 증가는 나쁜 것
          }
        }
        return null;
      },
    },
  ];

  // 테이블 데이터
  const rowData = [
    {
      자재그룹명: "6000 SYSTEM(BP제외)",
      "2024-09": 1361861984,
      "2024-10": 1381861984,
      "2024-11": 1361861984,
      "2024-12": 1361861984,
      "2025-01": 1361861984,
      "2025-02": 1446297182,
      재고비중: "16.09%",
      증감율: "+6%",
    },
    {
      자재그룹명: "DIGITAL AMP",
      "2024-09": 637938733,
      "2024-10": 637938733,
      "2024-11": 637938733,
      "2024-12": 637938733,
      "2025-01": 637938733,
      "2025-02": 980428583,
      재고비중: "10.91%",
      증감율: "+54%",
    },
    {
      자재그룹명: "CEILING SPK",
      "2024-09": 706826195,
      "2024-10": 706826195,
      "2024-11": 706826195,
      "2024-12": 706826195,
      "2025-01": 706826195,
      "2025-02": 854618761,
      재고비중: "9.51%",
      증감율: "+21%",
    },
    {
      자재그룹명: "전송 SOLUTION",
      "2024-09": 722382503,
      "2024-10": 722382503,
      "2024-11": 722382503,
      "2024-12": 722382503,
      "2025-01": 722382503,
      "2025-02": 544684429,
      재고비중: "6.06%",
      증감율: "-25%",
    },
    {
      자재그룹명: "COLUMN SPK",
      "2024-09": 484241033,
      "2024-10": 484241033,
      "2024-11": 484241033,
      "2024-12": 484241033,
      "2025-01": 484241033,
      "2025-02": 529622507,
      재고비중: "5.89%",
      증감율: "+9%",
    },
    {
      자재그룹명: "WALL SPK",
      "2024-09": 222055855,
      "2024-10": 222055855,
      "2024-11": 222055855,
      "2024-12": 222055855,
      "2025-01": 222055855,
      "2025-02": 380627906,
      재고비중: "4.23%",
      증감율: "+71%",
    },
    {
      자재그룹명: "ETC",
      "2024-09": 401566267,
      "2024-10": 401566267,
      "2024-11": 401566267,
      "2024-12": 401566267,
      "2025-01": 401566267,
      "2025-02": 360893238,
      재고비중: "4.01%",
      증감율: "-10%",
    },
    {
      자재그룹명: "MIXER AMP",
      "2024-09": 494719396,
      "2024-10": 494719396,
      "2024-11": 494719396,
      "2024-12": 494719396,
      "2025-01": 494719396,
      "2025-02": 336934894,
      재고비중: "3.75%",
      증감율: "-32%",
    },
    {
      자재그룹명: "SR SPK",
      "2024-09": 323845696,
      "2024-10": 323845696,
      "2024-11": 323845696,
      "2024-12": 323845696,
      "2025-01": 323845696,
      "2025-02": 327449379,
      재고비중: "3.64%",
      증감율: "+1%",
    },
    {
      자재그룹명: "SOURCE",
      "2024-09": 316992826,
      "2024-10": 316992826,
      "2024-11": 316992826,
      "2024-12": 316992826,
      "2025-01": 316992826,
      "2025-02": 318563065,
      재고비중: "3.54%",
      증감율: "+0%",
    },
    {
      자재그룹명: "A/V SOLUTION",
      "2024-09": 323602375,
      "2024-10": 323602375,
      "2024-11": 323602375,
      "2024-12": 323602375,
      "2025-01": 323602375,
      "2025-02": 297157848,
      재고비중: "3.31%",
      증감율: "-8%",
    },
    {
      자재그룹명: "RX-TX SYSTEM",
      "2024-09": 580014195,
      "2024-10": 580014195,
      "2024-11": 580014195,
      "2024-12": 580014195,
      "2025-01": 580014195,
      "2025-02": 268408475,
      재고비중: "2.99%",
      증감율: "-54%",
    },
    {
      자재그룹명: "PROJECTOR",
      "2024-09": 291453049,
      "2024-10": 291453049,
      "2024-11": 291453049,
      "2024-12": 291453049,
      "2025-01": 291453049,
      "2025-02": 263894564,
      재고비중: "2.94%",
      증감율: "-9%",
    },
    {
      자재그룹명: "RAIL PROJECT",
      "2024-09": 434626299,
      "2024-10": 434626299,
      "2024-11": 434626299,
      "2024-12": 434626299,
      "2025-01": 434626299,
      "2025-02": 184613078,
      재고비중: "2.05%",
      증감율: "-58%",
    },
    {
      자재그룹명: "FASHION SPK",
      "2024-09": 251299317,
      "2024-10": 251299317,
      "2024-11": 251299317,
      "2024-12": 251299317,
      "2025-01": 251299317,
      "2025-02": 183813184,
      재고비중: "2.04%",
      증감율: "-27%",
    },
    {
      자재그룹명: "이하생략 (5000 RAIL제외)",
      "2024-09": 3088522455,
      "2024-10": 3132250365,
      "2024-11": 3088522455,
      "2024-12": 3088522455,
      "2025-01": 3088522455,
      "2025-02": 3088522455,
      재고비중: "34.36%",
      증감율: "+0%",
    },
    {
      자재그룹명: "총 합계",
      "2024-09": 8989933646,
      "2024-10": 8989933646,
      "2024-11": 8989933646,
      "2024-12": 8989933646,
      "2025-01": 8989933646,
      "2025-02": 8989933646,
      재고비중: "100.0%",
      증감율: "",
    },
  ];

  console.log("rowData:", rowData);

  return (
    <div className="flex flex-col gap-6">
      {/* 헤더 섹션 */}
      <div className="flex items-center justify-between">
        <h1 className="flex items-center gap-4">
          <span className="text-2xl font-bold">제품군별 재고 현황</span>
        </h1>
        <div className="flex items-center gap-2">
          <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="기준년월" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2025.02">2025.02</SelectItem>
              <SelectItem value="2025.01">2025.01</SelectItem>
              <SelectItem value="2024.12">2024.12</SelectItem>
            </SelectContent>
          </Select>
          <Select>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="부서명" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">전체</SelectItem>
              <SelectItem value="domestic">국내영업</SelectItem>
              <SelectItem value="overseas">해외영업</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* 테이블 */}
      <AppAgGrid
        rowData={rowData}
        columnDefs={columnDefs}
        defaultColDef={{
          resizable: true,
          sortable: true,
        }}
        headerHeight={40}
        rowHeight={36}
        title=""
        showPagination={false}
        getRowStyle={(params) => {
          if (params.data?.자재그룹명 === "총 합계") {
            return { backgroundColor: "#f3f4f6", fontWeight: "bold" };
          }
          return null;
        }}
      />
    </div>
  );
}
