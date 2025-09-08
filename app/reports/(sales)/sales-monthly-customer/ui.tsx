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
      field: "고객명",
      headerName: "고객명",
      width: 150,
      pinned: "left",
    },
    {
      field: "영업그룹",
      headerName: "영업그룹",
      width: 100,
    },
    {
      field: "고객구분",
      headerName: "고객구분",
      width: 80,
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
        if (params.value) {
          return `${params.value}%`;
        }
        return "0%";
      },
      cellStyle: (params: any) => {
        if (params.value && params.value < 0) {
          return { color: "#ef4444" };
        }
        return null;
      },
    },
    {
      field: "증감율(전년)",
      headerName: "증감율(전년)",
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
      고객명: "(주)동아STAVE",
      영업그룹: "서울1/대주",
      고객구분: "VIP",
      "38차": 39516800,
      "39차": 148929700,
      "40차": 209951200,
      "41차": 488835400,
      "매출목표(당해차수)": 537718940,
      "매출실적(당해차수)": 308208480,
      달성률: -37,
      "증감율(전년)": 1544,
    },
    {
      고객명: "(주)동아자연환경(H&V)",
      영업그룹: "서울2",
      고객구분: "VIP",
      "38차": 138731600,
      "39차": 99339870,
      "40차": 257974580,
      "41차": 30634500,
      "매출목표(당해차수)": 33697950,
      "매출실적(당해차수)": 503765400,
      달성률: 1495,
      "증감율(전년)": 1544,
    },
    {
      고객명: "성남동천지구",
      영업그룹: "서울3",
      고객구분: "VIP",
      "38차": 188779100,
      "39차": 197846200,
      "40차": 110606500,
      "41차": 150829200,
      "매출목표(당해차수)": 165912120,
      "매출실적(당해차수)": 154410800,
      달성률: 93,
      "증감율(전년)": 2,
    },
    {
      고객명: "세한인쇄로마콘추리희사",
      영업그룹: "경기/인천",
      고객구분: "VIP",
      "38차": 90473600,
      "39차": 124083500,
      "40차": 152054100,
      "41차": 41107400,
      "매출목표(당해차수)": 45218140,
      "매출실적(당해차수)": 73503700,
      달성률: 163,
      "증감율(전년)": 79,
    },
    {
      고객명: "동화전자",
      영업그룹: "부산/경남",
      고객구분: "VIP",
      "38차": 78988300,
      "39차": 143566300,
      "40차": 138197700,
      "41차": 50045800,
      "매출목표(당해차수)": 55050380,
      "매출실적(당해차수)": 64095900,
      달성률: 116,
      "증감율(전년)": 28,
    },
    {
      고객명: "동원로익적(주)",
      영업그룹: "광주/호남",
      고객구분: "VIP",
      "38차": 55951600,
      "39차": 84669300,
      "40차": 89896800,
      "41차": 94558400,
      "매출목표(당해차수)": 104014240,
      "매출실적(당해차수)": 91266200,
      달성률: 88,
      "증감율(전년)": -3,
    },
    {
      고객명: "청도전자",
      영업그룹: "대구/경북",
      고객구분: "VIP",
      "38차": 60458800,
      "39차": 57769300,
      "40차": 135978300,
      "41차": 65355000,
      "매출목표(당해차수)": 71890500,
      "매출실적(당해차수)": 70903000,
      달성률: 99,
      "증감율(전년)": 8,
    },
    {
      고객명: "주식회사 티엔인쇄",
      영업그룹: "대전/충청/강원",
      고객구분: "일반",
      "38차": 33048100,
      "39차": 56128000,
      "40차": 96464000,
      "41차": 78537200,
      "매출목표(당해차수)": 86390920,
      "매출실적(당해차수)": 82167600,
      달성률: 95,
      "증감율(전년)": 5,
    },
    {
      고객명: "주식회사 이알컴아이엔",
      영업그룹: "서울1/대주",
      고객구분: "일반",
      "38차": 35943700,
      "39차": 161026400,
      "40차": 62653550,
      "41차": 47610000,
      "매출목표(당해차수)": 52371000,
      "매출실적(당해차수)": 32402200,
      달성률: 62,
      "증감율(전년)": -32,
    },
    {
      고객명: "주식회사 동원사스텝",
      영업그룹: "서울2",
      고객구분: "일반",
      "38차": 46355200,
      "39차": 37677900,
      "40차": 57859200,
      "41차": 30504600,
      "매출목표(당해차수)": 33555060,
      "매출실적(당해차수)": 102664000,
      달성률: 306,
      "증감율(전년)": 217,
    },
    {
      고객명: "(주)에이앤컴",
      영업그룹: "서울3",
      고객구분: "일반",
      "38차": 54417000,
      "39차": 31701400,
      "40차": 92612000,
      "41차": 40431800,
      "매출목표(당해차수)": 44474980,
      "매출실적(당해차수)": 8173600,
      달성률: 18,
      "증감율(전년)": -80,
    },
    {
      고객명: "(주)선흥철재상",
      영업그룹: "경기/인천",
      고객구분: "일반",
      "38차": 50826200,
      "39차": 62075400,
      "40차": 43701000,
      "41차": 66431600,
      "매출목표(당해차수)": 73074760,
      "매출실적(당해차수)": "",
      달성률: 0,
      "증감율(전년)": -100,
    },
    {
      고객명: "(주)삼인테딘",
      영업그룹: "부산/경남",
      고객구분: "일반",
      "38차": 38588400,
      "39차": 31961600,
      "40차": 69532600,
      "41차": 35804000,
      "매출목표(당해차수)": 39384400,
      "매출실적(당해차수)": 29258000,
      달성률: 74,
      "증감율(전년)": -18,
    },
    {
      고객명: "이주전자",
      영업그룹: "광주/호남",
      고객구분: "일반",
      "38차": 36728800,
      "39차": 47118000,
      "40차": 49773200,
      "41차": 54207600,
      "매출목표(당해차수)": 37628360,
      "매출실적(당해차수)": 36149200,
      달성률: 96,
      "증감율(전년)": 0,
    },
    {
      고객명: "대성크린시스템",
      영업그룹: "대구/경북",
      고객구분: "일반",
      "38차": 17528800,
      "39차": 55456300,
      "40차": 26685600,
      "41차": 24184800,
      "매출목표(당해차수)": 26558180,
      "매출실적(당해차수)": 80162200,
      달성률: 302,
      "증감율(전년)": 232,
    },
    {
      고객명: "(주)성문컬포론션",
      영업그룹: "대전/충청/강원",
      고객구분: "일반",
      "38차": 91883700,
      "39차": 36021800,
      "40차": 23216800,
      "41차": 16114600,
      "매출목표(당해차수)": 17726060,
      "매출실적(당해차수)": 20284000,
      달성률: 114,
      "증감율(전년)": 26,
    },
    {
      고객명: "에스퍼 비젠시즈펜(주, 호윤신)",
      영업그룹: "서울1/대주",
      고객구분: "일반",
      "38차": 38704000,
      "39차": 38693600,
      "40차": 11259400,
      "41차": 11197000,
      "매출목표(당해차수)": 12316700,
      "매출실적(당해차수)": 41014400,
      달성률: 333,
      "증감율(전년)": 266,
    },
    {
      고객명: "주식회사 중앙인권",
      영업그룹: "서울2",
      고객구분: "일반",
      "38차": 20059800,
      "39차": 56020100,
      "40차": 7891000,
      "41차": 1634000,
      "매출목표(당해차수)": 1797400,
      "매출실적(당해차수)": 43472200,
      달성률: 2419,
      "증감율(전년)": 2560,
    },
    {
      고객명: "(주)에스브링",
      영업그룹: "서울3",
      고객구분: "일반",
      "38차": 9909900,
      "39차": 32487000,
      "40차": 64352100,
      "41차": 14147800,
      "매출목표(당해차수)": 15562580,
      "매출실적(당해차수)": 58352500,
      달성률: 375,
      "증감율(전년)": 312,
    },
    {
      고객명: "수미서울동권투센터",
      영업그룹: "경기/인천",
      고객구분: "일반",
      "38차": 135728700,
      "39차": 4794000,
      "40차": 2888500,
      "41차": 12571800,
      "매출목표(당해차수)": 13828760,
      "매출실적(당해차수)": 21198500,
      달성률: 153,
      "증감율(전년)": 69,
    },
    {
      고객명: "(주)세원모",
      영업그룹: "부산/경남",
      고객구분: "일반",
      "38차": 9790600,
      "39차": 18567800,
      "40차": 26411000,
      "41차": 44826500,
      "매출목표(당해차수)": 49309150,
      "매출실적(당해차수)": 51549800,
      달성률: 105,
      "증감율(전년)": 15,
    },
    {
      고객명: "주식회사 에이앤엠비보린",
      영업그룹: "광주/호남",
      고객구분: "일반",
      "38차": 7316700,
      "39차": 73316700,
      "40차": 36158800,
      "41차": 4992000,
      "매출목표(당해차수)": 5491200,
      "매출실적(당해차수)": 31049600,
      달성률: 565,
      "증감율(전년)": 522,
    },
    {
      고객명: "에스피지케",
      영업그룹: "대구/경북",
      고객구분: "일반",
      "38차": 38622800,
      "39차": 58399300,
      "40차": 15451100,
      "41차": 6666700,
      "매출목표(당해차수)": 7333370,
      "매출실적(당해차수)": 26326000,
      달성률: 359,
      "증감율(전년)": 295,
    },
    {
      고객명: "공립사무",
      영업그룹: "대전/충청/강원",
      고객구분: "일반",
      "38차": 20109400,
      "39차": 44390900,
      "40차": 62900400,
      "41차": 4899200,
      "매출목표(당해차수)": 5389120,
      "매출실적(당해차수)": 8053200,
      달성률: 149,
      "증감율(전년)": 64,
    },
    {
      고객명: "민지물플링장보관(주)",
      영업그룹: "부산/경남",
      고객구분: "일반",
      "38차": 27767200,
      "39차": 8316400,
      "40차": 29349300,
      "41차": 45923800,
      "매출목표(당해차수)": 50606180,
      "매출실적(당해차수)": 28974600,
      달성률: 57,
      "증감율(전년)": -37,
    },
    {
      고객명: "(주)경호홍적",
      영업그룹: "광주/호남",
      고객구분: "일반",
      "38차": 11211300,
      "39차": 15577800,
      "40차": 56933200,
      "41차": 14792000,
      "매출목표(당해차수)": 16271200,
      "매출실적(당해차수)": 31445400,
      달성률: 193,
      "증감율(전년)": 113,
    },
    {
      고객명: "동아테그로론(주)",
      영업그룹: "대구/경북",
      고객구분: "일반",
      "38차": 39836000,
      "39차": 29111300,
      "40차": 27420700,
      "41차": 31806300,
      "매출목표(당해차수)": 34986930,
      "매출실적(당해차수)": "",
      달성률: 0,
      "증감율(전년)": -100,
    },
    {
      고객명: "애림엔성산업",
      영업그룹: "대전/충청/강원",
      고객구분: "일반",
      "38차": 10576500,
      "39차": 30239800,
      "40차": 22186200,
      "41차": 20867800,
      "매출목표(당해차수)": 22954580,
      "매출실적(당해차수)": 35539600,
      달성률: 155,
      "증감율(전년)": 70,
    },
  ];

  const getRowStyle = (params: any) => {};

  return (
    <div className="min-h-screen">
      <div className="flex justify-between items-start">
        <div className="flex items-center gap-4">
          <h1 className="text-2xl font-bold">고객별 매출 실적</h1>
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
              <Check className="w-4 h-4" />
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
