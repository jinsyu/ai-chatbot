"use client";

import AppAgGrid from "@/components/app-aggrid";
import type { ColDef } from "ag-grid-community";

export default function InventoryShortageUI() {
  // 테이블 컬럼 정의
  const columnDefs: ColDef[] = [
    {
      field: "자재번호",
      headerName: "자재번호",
      width: 100,
      pinned: "left",
    },
    {
      field: "자재명",
      headerName: "자재명",
      width: 150,
    },
    {
      field: "부족수량",
      headerName: "부족수량",
      width: 90,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "출하예정일",
      headerName: "출하 예정일",
      width: 110,
    },
    {
      field: "대응방식",
      headerName: "대응 방식",
      width: 100,
    },
    {
      field: "대응상태",
      headerName: "대응 상태",
      width: 90,
    },
    {
      field: "생산오더",
      headerName: "생산오더",
      width: 110,
      cellStyle: { color: "#ef4444" },
    },
    {
      field: "생산일고계획일",
      headerName: "생산/입고 계획일",
      width: 130,
    },
    {
      field: "대응수량",
      headerName: "대응수량",
      width: 90,
      valueFormatter: (params) => params.value?.toLocaleString() || "0",
    },
    {
      field: "생산입고확인일",
      headerName: "생산/입고 확인일",
      width: 130,
    },
    {
      field: "일정지연사유",
      headerName: "일정 지연 사유",
      width: 300,
    },
    {
      field: "비고",
      headerName: "비고",
      width: 150,
    },
  ];

  // 테이블 데이터
  const rowData = [
    {
      자재번호: "303588",
      자재명: "V2-5000(IMK) 시판",
      부족수량: 1,
      출하예정일: "4/24~5/7",
      대응방식: "생산(IMK)",
      대응상태: "입점 확인",
      생산오더: "2503NK094",
      생산일고계획일: "2025.04.11",
      대응수량: 6,
      생산입고확인일: "4/ 6LOT",
      일정지연사유: "25년 4월 2주차 생산오더 전체공정 필요자재 확인",
      비고: "",
    },
    {
      자재번호: "701129",
      자재명: "BP-1U 시판",
      부족수량: 0,
      출하예정일: "2/20~3/5",
      대응방식: "생산(IMP)",
      대응상태: "오더 등록",
      생산오더: "2503NP024",
      생산일고계획일: "2025.04.04",
      대응수량: 1200,
      생산입고확인일: "",
      일정지연사유: "",
      비고: "",
    },
    {
      자재번호: "",
      자재명: "",
      부족수량: "",
      출하예정일: "",
      대응방식: "",
      대응상태: "",
      생산오더: "2503NP340",
      생산일고계획일: "2025.04.25",
      대응수량: 1200,
      생산입고확인일: "",
      일정지연사유: "",
      비고: "",
    },
    {
      자재번호: "401630",
      자재명: "MD-710V 시판",
      부족수량: 38,
      출하예정일: "3/27~4/9",
      대응방식: "구매(IMK)",
      대응상태: "오더 부족",
      생산오더: "4500221849",
      생산일고계획일: "2025.04.04",
      대응수량: 30,
      생산입고확인일: "",
      일정지연사유: "",
      비고: "",
    },
    {
      자재번호: "470989",
      자재명: "CS-610M 시판",
      부족수량: 10,
      출하예정일: "",
      대응방식: "생산(IMC)",
      대응상태: "오더 미등록",
      생산오더: "",
      생산일고계획일: "",
      대응수량: "",
      생산입고확인일: "",
      일정지연사유: "",
      비고: "",
    },
  ];

  return (
    <div className="flex flex-col gap-6">
      {/* 헤더 섹션 */}
      <div className="flex items-center justify-between">
        <h1 className="flex items-center gap-4">
          <span className="text-2xl font-bold">재고 부족 현황</span>
        </h1>
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
          // 470989 행은 연한 주황색 배경
          if (params.data?.자재번호 === "470989") {
            return { backgroundColor: "#fee2e2" };
          }
          return null;
        }}
      />
    </div>
  );
}