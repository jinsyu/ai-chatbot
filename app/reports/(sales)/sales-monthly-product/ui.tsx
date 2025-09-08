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
      field: "제품명",
      headerName: "제품명",
      width: 100,
      pinned: "left",
    },
    {
      field: "자재그룹7",
      headerName: "자재그룹7",
      width: 90,
    },
    {
      field: "자재그룹6",
      headerName: "자재그룹6",
      width: 90,
    },
    {
      field: "자재그룹8",
      headerName: "자재그룹8",
      width: 90,
    },
    {
      field: "자재그룹9",
      headerName: "자재그룹9",
      width: 90,
    },
    {
      headerName: "매출실적(38차)",
      field: "38차",
      width: 110,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(39차)",
      field: "39차",
      width: 110,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(40차)",
      field: "40차",
      width: 110,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출실적(41차)",
      field: "41차",
      width: 110,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
    },
    {
      headerName: "매출목표(당해차수)",
      field: "매출목표",
      width: 130,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
      cellStyle: { backgroundColor: "#fef3c7" },
      headerClass: "ag-header-cell-highlight",
    },
    {
      headerName: "매출실적(당해차수)",
      field: "매출실적",
      width: 130,
      valueFormatter: (params: any) => params.value?.toLocaleString() || "0",
      cellStyle: { backgroundColor: "#fef3c7" },
      headerClass: "ag-header-cell-highlight",
    },
    {
      field: "달성률",
      headerName: "달성률",
      width: 80,
      valueFormatter: (params: any) => {
        if (params.value) {
          return `${params.value}%`;
        }
        return "0%";
      },
      cellStyle: (params: any) => {
        if (params.value && params.value < 100) {
          return { color: "#ef4444" };
        }
        return null;
      },
    },
    {
      field: "증감률(전년)",
      headerName: "증감률(전년)",
      width: 100,
      valueFormatter: (params: any) => {
        if (params.value) {
          const sign = params.value >= 0 ? "+" : "";
          return `${sign}${params.value}%`;
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
      제품명: "CS-610",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 120099000,
      "39차": 190198000,
      "40차": 147022700,
      "41차": 114683800,
      매출목표: 126152180,
      매출실적: 137141400,
      달성률: 109,
      "증감률(전년)": 20,
    },
    {
      제품명: "CS-503F",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 76122000,
      "39차": 48724000,
      "40차": 116360000,
      "41차": 80223000,
      매출목표: 88245300,
      매출실적: 97988000,
      달성률: 111,
      "증감률(전년)": 22,
    },
    {
      제품명: "PD-6359",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 56049000,
      "39차": 39920100,
      "40차": 72074400,
      "41차": 52836000,
      매출목표: 58119600,
      매출실적: 73780000,
      달성률: 127,
      "증감률(전년)": 40,
    },
    {
      제품명: "CS-610FA",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 48672000,
      "39차": 42473400,
      "40차": 69349500,
      "41차": 43126200,
      매출목표: 47438820,
      매출실적: 62899200,
      달성률: 133,
      "증감률(전년)": 46,
    },
    {
      제품명: "CU-41F",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 33973000,
      "39차": 43173500,
      "40차": 81613600,
      "41차": 69489200,
      매출목표: 76438120,
      매출실적: 37961000,
      달성률: 50,
      "증감률(전년)": -45,
    },
    {
      제품명: "CS-301FC",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 34503000,
      "39차": 35392600,
      "40차": 65151800,
      "41차": 60593400,
      매출목표: 66652740,
      매출실적: 56154000,
      달성률: 84,
      "증감률(전년)": -7,
    },
    {
      제품명: "ARX-5132S",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 29750000,
      "39차": 43587500,
      "40차": 49419000,
      "41차": 67337000,
      매출목표: 74070700,
      매출실적: 54332000,
      달성률: 73,
      "증감률(전년)": -19,
    },
    {
      제품명: "ECS-6216P",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 28003000,
      "39차": 69269300,
      "40차": 67319000,
      "41차": 30030000,
      매출목표: 33033000,
      매출실적: 44590000,
      달성률: 135,
      "증감률(전년)": 48,
    },
    {
      제품명: "ARX-5132",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 39790000,
      "39차": 27870500,
      "40차": 105602000,
      "41차": 19950000,
      매출목표: 21945000,
      매출실적: 16492000,
      달성률: 75,
      "증감률(전년)": -17,
    },
    {
      제품명: "AMC-3300",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 63800000,
      "39차": 62132500,
      "40차": 15075000,
      "41차": 35175000,
      매출목표: 38692500,
      매출실적: 31825000,
      달성률: 82,
      "증감률(전년)": -10,
    },
    {
      제품명: "PA-6348",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 21460000,
      "39차": 38977000,
      "40차": 60903000,
      "41차": 13266000,
      매출목표: 14592600,
      매출실적: 42746000,
      달성률: 293,
      "증감률(전년)": 222,
    },
    {
      제품명: "PA-6324",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 23100000,
      "39차": 40635000,
      "40차": 30766600,
      "41차": 29425000,
      매출목표: 32367500,
      매출실적: 47080000,
      달성률: 145,
      "증감률(전년)": 60,
    },
    {
      제품명: "PA-6336",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 17000000,
      "39차": 34030000,
      "40차": 51616000,
      "41차": 17172000,
      매출목표: 18889200,
      매출실적: 38796000,
      달성률: 205,
      "증감률(전년)": 126,
    },
    {
      제품명: "PP-6213",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 27359000,
      "39차": 26583800,
      "40차": 41711200,
      "41차": 33500000,
      매출목표: 36850000,
      매출실적: 27470000,
      달성률: 75,
      "증감률(전년)": -18,
    },
    {
      제품명: "CU-42FO",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 19775000,
      "39차": 27771200,
      "40차": 34749000,
      "41차": 26680500,
      매출목표: 29348550,
      매출실적: 43015500,
      달성률: 147,
      "증감률(전년)": 61,
    },
    {
      제품명: "SC-6216C",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 29500000,
      "39차": 38450000,
      "40차": 38720000,
      "41차": 18150000,
      매출목표: 19965000,
      매출실적: 25410000,
      달성률: 127,
      "증감률(전년)": 40,
    },
    {
      제품명: "CS-303FC",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 22110000,
      "39차": 22300800,
      "40차": 24158580,
      "41차": 61597400,
      매출목표: 67757140,
      매출실적: 19053000,
      달성률: 28,
      "증감률(전년)": -69,
    },
    {
      제품명: "SWS-03(I)",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 19480400,
      "39차": 29530800,
      "40차": 28810600,
      "41차": 33664400,
      매출목표: 37030840,
      매출실적: 34896400,
      달성률: 94,
      "증감률(전년)": 4,
    },
    {
      제품명: "CD-610U",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 26378000,
      "39차": 19170800,
      "40차": 43371000,
      "41차": 22610000,
      매출목표: 24871000,
      매출실적: 33592000,
      달성률: 135,
      "증감률(전년)": 49,
    },
    {
      제품명: "SPAC-S9",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 80640000,
      "39차": 8507000,
      "40차": 14542000,
      "41차": 6736000,
      매출목표: 7409600,
      매출실적: 23068000,
      달성률: 311,
      "증감률(전년)": 242,
    },
    {
      제품명: "ECS-6216S",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 14700000,
      "39차": 23107500,
      "40차": 24296000,
      "41차": 23800000,
      매출목표: 26180000,
      매출실적: 23800000,
      달성률: 91,
      "증감률(전년)": 0,
    },
    {
      제품명: "EP-6216",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 18426000,
      "39차": 15181400,
      "40차": 20310000,
      "41차": 24420000,
      매출목표: 26862000,
      매출실적: 30636000,
      달성률: 114,
      "증감률(전년)": 25,
    },
    {
      제품명: "PB-6207",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 17542000,
      "39차": 17877100,
      "40차": 35244400,
      "41차": 18164000,
      매출목표: 19980400,
      매출실적: 17447000,
      달성률: 87,
      "증감률(전년)": -4,
    },
    {
      제품명: "RM-6024",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 9455000,
      "39차": 21325000,
      "40차": 30451000,
      "41차": 26455000,
      매출목표: 29100500,
      매출실적: 17908000,
      달성률: 62,
      "증감률(전년)": -32,
    },
    {
      제품명: "ASC-5116",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 9750000,
      "39차": 34515000,
      "40차": 20277000,
      "41차": 23281000,
      매출목표: 25609100,
      매출실적: 17273000,
      달성률: 67,
      "증감률(전년)": -26,
    },
    {
      제품명: "PA-224",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 2632000,
      "39차": 14114200,
      "40차": 16000000,
      "41차": 28600000,
      매출목표: 31460000,
      매출실적: 43560000,
      달성률: 138,
      "증감률(전년)": 52,
    },
    {
      제품명: "ECS-6216MS",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 4320000,
      "39차": 23048000,
      "40차": 43704000,
      "41차": 20160000,
      매출목표: 22176000,
      매출실적: 12480000,
      달성률: 56,
      "증감률(전년)": -38,
    },
    {
      제품명: "CS-301FC(W)",
      자재그룹7: "",
      자재그룹6: "",
      자재그룹8: "",
      자재그룹9: "",
      "38차": 8225000,
      "39차": 35590000,
      "40차": 17300800,
      "41차": 8251600,
      매출목표: 9076760,
      매출실적: 30982700,
      달성률: 341,
      "증감률(전년)": 275,
    },
  ];

  const getRowStyle = () => {};

  return (
    <div className="min-h-screen">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">제품별 매출 실적</h1>
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
