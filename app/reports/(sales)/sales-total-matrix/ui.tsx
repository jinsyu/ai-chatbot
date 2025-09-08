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
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Check } from "lucide-react";

export default function MonthlyReportUI() {
  const [selectedPeriod, setSelectedPeriod] = useState("42차");

  const columnDefs: (ColDef | ColGroupDef)[] = [
    {
      field: "자재그룹7",
      headerName: "자재그룹7",
      width: 100,
      pinned: "left",
    },
    {
      field: "자재그룹6",
      headerName: "자재그룹6",
      width: 100,
    },
    {
      field: "자재그룹8",
      headerName: "자재그룹8",
      width: 100,
    },
    {
      field: "자재그룹9",
      headerName: "자재그룹9",
      width: 100,
    },
    {
      headerName: "매출실적(38차)",
      field: "38차",
      width: 100,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(39차)",
      field: "39차",
      width: 100,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(40차)",
      field: "40차",
      width: 100,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(41차)",
      field: "41차",
      width: 100,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출목표(당해차수)",
      field: "매출목표(당해차수)",
      width: 150,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
      cellStyle: { backgroundColor: "#fef3c7" },
      headerClass: "ag-header-cell-highlight",
    },
    {
      headerName: "매출실적(당해차수)",
      field: "매출실적(당해차수)",
      width: 150,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
      cellStyle: { backgroundColor: "#fef3c7" },
      headerClass: "ag-header-cell-highlight",
    },
    {
      field: "달성률",
      headerName: "달성률",
      width: 80,
      valueFormatter: (params: any) => {
        if (params.value !== null && params.value !== undefined) {
          const sign = params.value >= 0 ? "+" : "";
          return `${sign}${params.value}%`;
        }
        return "0%";
      },
      cellStyle: (params: any) => {
        if (params.value && params.value < 0) {
          return { color: "#ef4444" };
        } else if (params.value && params.value > 0) {
          return { color: "#10b981" };
        }
        return null;
      },
    },
    {
      field: "증감률(전년)",
      headerName: "증감률(전년)",
      width: 100,
      valueFormatter: (params: any) => {
        if (params.value !== null && params.value !== undefined) {
          return `${params.value}%`;
        }
        return "0%";
      },
      cellStyle: (params: any) => {
        if (params.value) {
          if (params.value < 0) {
            return { color: "#ef4444" };
          } else if (params.value > 0) {
            return { color: "#10b981" };
          }
        }
        return null;
      },
    },
  ];

  const rowData = [
    {
      자재그룹7: "PA",
      자재그룹6: "소스",
      자재그룹8: "무선마이크",
      자재그룹9: "900MHz",
      "38차": 48277000,
      "39차": 48277000,
      "40차": 48277000,
      "41차": 48277000,
      "매출목표(당해차수)": 48277000,
      "매출실적(당해차수)": 62019272,
      달성률: 28,
      "증감율(전년)": 117,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "ETC",
      자재그룹9: "",
      "38차": 292000,
      "39차": 292000,
      "40차": 292000,
      "41차": 292000,
      "매출목표(당해차수)": 292000,
      "매출실적(당해차수)": 0,
      달성률: -100,
      "증감율(전년)": 0,
    },
    {
      자재그룹7: "",
      자재그룹6: "CD/레코더",
      자재그룹8: "CD/레코더",
      자재그룹9: "",
      "38차": 25140000,
      "39차": 25140000,
      "40차": 25140000,
      "41차": 25140000,
      "매출목표(당해차수)": 25140000,
      "매출실적(당해차수)": 44724000,
      달성률: 78,
      "증감율(전년)": 162,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "리모트",
      자재그룹9: "6000",
      "38차": 33585000,
      "39차": 33585000,
      "40차": 33585000,
      "41차": 33585000,
      "매출목표(당해차수)": 33585000,
      "매출실적(당해차수)": 21588000,
      달성률: -36,
      "증감율(전년)": 58,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "RM Series",
      "38차": 848000,
      "39차": 848000,
      "40차": 848000,
      "41차": 848000,
      "매출목표(당해차수)": 848000,
      "매출실적(당해차수)": 2544000,
      달성률: 200,
      "증감율(전년)": 273,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "RX-TX 3000",
      "38차": 4510000,
      "39차": 4510000,
      "40차": 4510000,
      "41차": 4510000,
      "매출목표(당해차수)": 4510000,
      "매출실적(당해차수)": 6560000,
      달성률: 45,
      "증감율(전년)": 132,
    },
    {
      자재그룹7: "",
      자재그룹6: "타이머",
      자재그룹8: "6000",
      자재그룹9: "",
      "38차": 16694000,
      "39차": 16694000,
      "40차": 16694000,
      "41차": 16694000,
      "매출목표(당해차수)": 16694000,
      "매출실적(당해차수)": 20622000,
      달성률: 24,
      "증감율(전년)": 112,
    },
    {
      자재그룹7: "",
      자재그룹6: "유선마이크",
      자재그룹8: "MC Series",
      자재그룹9: "",
      "38차": 140000,
      "39차": 140000,
      "40차": 140000,
      "41차": 140000,
      "매출목표(당해차수)": 140000,
      "매출실적(당해차수)": 280000,
      달성률: 100,
      "증감율(전년)": 182,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "MD Series",
      자재그룹9: "",
      "38차": 8772000,
      "39차": 8772000,
      "40차": 8772000,
      "41차": 8772000,
      "매출목표(당해차수)": 8772000,
      "매출실적(당해차수)": 7912000,
      달성률: -10,
      "증감율(전년)": 82,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "NMD Series",
      자재그룹9: "",
      "38차": 1225000,
      "39차": 1225000,
      "40차": 1225000,
      "41차": 1225000,
      "매출목표(당해차수)": 1225000,
      "매출실적(당해차수)": 420000,
      달성률: -66,
      "증감율(전년)": 31,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "RM Series",
      자재그룹9: "",
      "38차": 7765000,
      "39차": 7765000,
      "40차": 7765000,
      "41차": 7765000,
      "매출목표(당해차수)": 7765000,
      "매출실적(당해차수)": 7001000,
      달성률: -10,
      "증감율(전년)": 82,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "SCM Series",
      자재그룹9: "",
      "38차": 0,
      "39차": 0,
      "40차": 0,
      "41차": 0,
      "매출목표(당해차수)": 0,
      "매출실적(당해차수)": 104000,
      달성률: 0,
      "증감율(전년)": 0,
    },
    {
      자재그룹7: "",
      자재그룹6: "튜너",
      자재그룹8: "튜너",
      자재그룹9: "",
      "38차": 15075000,
      "39차": 15075000,
      "40차": 15075000,
      "41차": 15075000,
      "매출목표(당해차수)": 15075000,
      "매출실적(당해차수)": 18000000,
      달성률: 19,
      "증감율(전년)": 109,
    },
    {
      자재그룹7: "",
      자재그룹6: "상품",
      자재그룹8: "신화정공",
      자재그룹9: "",
      "38차": 27000000,
      "39차": 27000000,
      "40차": 27000000,
      "41차": 27000000,
      "매출목표(당해차수)": 27000000,
      "매출실적(당해차수)": 0,
      달성률: -100,
      "증감율(전년)": 0,
    },
    {
      자재그룹7: "",
      자재그룹6: "차임사이렌",
      자재그룹8: "6000",
      자재그룹9: "",
      "38차": 11524000,
      "39차": 11524000,
      "40차": 11524000,
      "41차": 11524000,
      "매출목표(당해차수)": 11524000,
      "매출실적(당해차수)": 12998000,
      달성률: 13,
      "증감율(전년)": 103,
    },
    {
      자재그룹7: "",
      자재그룹6: "멀티소스 플레이어",
      자재그룹8: "6000",
      자재그룹9: "",
      "38차": 920000,
      "39차": 920000,
      "40차": 920000,
      "41차": 920000,
      "매출목표(당해차수)": 920000,
      "매출실적(당해차수)": 6440000,
      달성률: 600,
      "증감율(전년)": 636,
    },
    {
      자재그룹7: "",
      자재그룹6: "이펙트 플레이어",
      자재그룹8: "6000",
      자재그룹9: "",
      "38차": 2678000,
      "39차": 2678000,
      "40차": 2678000,
      "41차": 2678000,
      "매출목표(당해차수)": 2678000,
      "매출실적(당해차수)": 1854000,
      달성률: -31,
      "증감율(전년)": 63,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "보이스파일",
      자재그룹9: "",
      "38차": 1386000,
      "39차": 1386000,
      "40차": 1386000,
      "41차": 1386000,
      "매출목표(당해차수)": 1386000,
      "매출실적(당해차수)": 0,
      달성률: -100,
      "증감율(전년)": 0,
    },
    {
      자재그룹7: "컨트롤러",
      자재그룹6: "RX/TX",
      자재그룹8: "RX-TX 3000",
      자재그룹9: "",
      "38차": 179534000,
      "39차": 179534000,
      "40차": 179534000,
      "41차": 179534000,
      "매출목표(당해차수)": 179534000,
      "매출실적(당해차수)": 144569000,
      달성률: -19,
      "증감율(전년)": 73,
    },
    {
      자재그룹7: "",
      자재그룹6: "6000",
      자재그룹8: "Software",
      자재그룹9: "",
      "38차": 19100000,
      "39차": 19100000,
      "40차": 19100000,
      "41차": 19100000,
      "매출목표(당해차수)": 19100000,
      "매출실적(당해차수)": 21600000,
      달성률: 13,
      "증감율(전년)": 103,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "입력",
      자재그룹9: "",
      "38차": 9059000,
      "39차": 9059000,
      "40차": 9059000,
      "41차": 9059000,
      "매출목표(당해차수)": 9059000,
      "매출실적(당해차수)": 9617000,
      달성률: 6,
      "증감율(전년)": 97,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "제어",
      자재그룹9: "",
      "38차": 124168000,
      "39차": 124168000,
      "40차": 124168000,
      "41차": 124168000,
      "매출목표(당해차수)": 124168000,
      "매출실적(당해차수)": 136835000,
      달성률: 10,
      "증감율(전년)": 100,
    },
    {
      자재그룹7: "공용",
      자재그룹6: "",
      자재그룹8: "모니터링",
      자재그룹9: "",
      "38차": 15886000,
      "39차": 15886000,
      "40차": 15886000,
      "41차": 15886000,
      "매출목표(당해차수)": 15886000,
      "매출실적(당해차수)": 12506000,
      달성률: -21,
      "증감율(전년)": 72,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "스피커라인체킹,",
      자재그룹9: "",
      "38차": 33463000,
      "39차": 33463000,
      "40차": 33463000,
      "41차": 33463000,
      "매출목표(당해차수)": 33463000,
      "매출실적(당해차수)": 59473000,
      달성률: 78,
      "증감율(전년)": 162,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "오디오분배",
      자재그룹9: "",
      "38차": 9424000,
      "39차": 9424000,
      "40차": 9424000,
      "41차": 9424000,
      "매출목표(당해차수)": 9424000,
      "매출실적(당해차수)": 19456000,
      달성률: 106,
      "증감율(전년)": 188,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "입력",
      자재그룹9: "",
      "38차": 37310000,
      "39차": 37310000,
      "40차": 37310000,
      "41차": 37310000,
      "매출목표(당해차수)": 37310000,
      "매출실적(당해차수)": 36233000,
      달성률: -3,
      "증감율(전년)": 88,
    },
    {
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "제어",
      자재그룹9: "",
      "38차": 12065000,
      "39차": 12065000,
      "40차": 12065000,
      "41차": 12065000,
      "매출목표(당해차수)": 12065000,
      "매출실적(당해차수)": 30870000,
      달성률: 156,
      "증감율(전년)": 233,
    },
    {
      자재그룹7: "ARM",
      자재그룹6: "",
      자재그룹8: "제어",
      자재그룹9: "",
      "38차": 0,
      "39차": 0,
      "40차": 0,
      "41차": 0,
      "매출목표(당해차수)": 0,
      "매출실적(당해차수)": 811000,
      달성률: 0,
      "증감율(전년)": 0,
    },
  ];

  const getRowStyle = () => {};

  return (
    <div className="min-h-screen">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">종합 매트릭스</h1>
          <span className="text-lg font-medium">
            {selectedPeriod}차 : 2025.03 ~ 2025.03 (금액)
          </span>
        </div>
        <div className="flex items-center gap-4 mb-4">
          <div className="flex items-center gap-2">
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="부서명" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">부서명</SelectItem>
                <SelectItem value="41차">41차</SelectItem>
                <SelectItem value="40차">40차</SelectItem>
                <SelectItem value="39차">39차</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="자재그룹명" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">자재그룹명</SelectItem>
                <SelectItem value="41차">41차</SelectItem>
                <SelectItem value="40차">40차</SelectItem>
                <SelectItem value="39차">39차</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="제품명" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">제품명</SelectItem>
                <SelectItem value="41차">41차</SelectItem>
                <SelectItem value="40차">40차</SelectItem>
                <SelectItem value="39차">39차</SelectItem>
              </SelectContent>
            </Select>
            <Select value={selectedPeriod} onValueChange={setSelectedPeriod}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="기간" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="42차">기간</SelectItem>
                <SelectItem value="41차">41차</SelectItem>
                <SelectItem value="40차">40차</SelectItem>
                <SelectItem value="39차">39차</SelectItem>
              </SelectContent>
            </Select>
            <Button className="flex items-center gap-2" variant="outline">
              <Label>수량보기</Label>
              <Check className="size-4" />
            </Button>
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
          suppressMovable: false,
          suppressHeaderMenuButton: true,
          cellStyle: {
            textAlign: "right",
            fontSize: "12px",
          },
        }}
        getRowStyle={getRowStyle}
        headerHeight={35}
        rowHeight={35}
        showPagination={false}
        title=""
        autoSizeStrategy={{
          type: "fitGridWidth",
        }}
      />

      <style jsx global>{`
        .ag-header-cell-highlight {
          background-color: #dbeafe !important;
        }
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
      `}</style>
    </div>
  );
}
