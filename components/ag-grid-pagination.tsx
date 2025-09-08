"use client";

import React, { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";
import { cn } from "@/lib/utils";

interface AgGridPaginationProps {
  gridApi: any;
  className?: string;
  pageSizeOptions?: number[];
}

export default function AgGridPagination({
  gridApi,
  className,
  pageSizeOptions = [10, 20, 50, 100],
}: AgGridPaginationProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [totalRows, setTotalRows] = useState(0);
  const [displayedRows, setDisplayedRows] = useState({ start: 0, end: 0 });

  // Grid API 이벤트 리스너 설정
  useEffect(() => {
    if (!gridApi) return;

    const updatePaginationInfo = () => {
      const totalRowCount = gridApi.getDisplayedRowCount();
      const paginationPageSize =
        gridApi.getGridOption("paginationPageSize") || 20;
      const currentPageNum = gridApi.paginationGetCurrentPage() + 1; // 0-based to 1-based
      const totalPageCount = gridApi.paginationGetTotalPages();

      setTotalRows(totalRowCount);
      setCurrentPage(currentPageNum);
      setTotalPages(totalPageCount);
      setPageSize(paginationPageSize);

      // 현재 표시되는 행 범위 계산
      const startRow = (currentPageNum - 1) * paginationPageSize + 1;
      const endRow = Math.min(
        currentPageNum * paginationPageSize,
        totalRowCount
      );
      setDisplayedRows({ start: startRow, end: endRow });
    };

    // 초기 정보 업데이트
    updatePaginationInfo();

    // 이벤트 리스너 등록
    gridApi.addEventListener("paginationChanged", updatePaginationInfo);
    gridApi.addEventListener("modelUpdated", updatePaginationInfo);

    return () => {
      gridApi.removeEventListener("paginationChanged", updatePaginationInfo);
      gridApi.removeEventListener("modelUpdated", updatePaginationInfo);
    };
  }, [gridApi]);

  // 페이지 이동 함수들
  const goToFirstPage = () => {
    if (gridApi && currentPage > 1) {
      gridApi.paginationGoToFirstPage();
    }
  };

  const goToPreviousPage = () => {
    if (gridApi && currentPage > 1) {
      gridApi.paginationGoToPreviousPage();
    }
  };

  const goToNextPage = () => {
    if (gridApi && currentPage < totalPages) {
      gridApi.paginationGoToNextPage();
    }
  };

  const goToLastPage = () => {
    if (gridApi && currentPage < totalPages) {
      gridApi.paginationGoToLastPage();
    }
  };

  const goToPage = (pageNumber: number) => {
    if (gridApi && pageNumber >= 1 && pageNumber <= totalPages) {
      gridApi.paginationGoToPage(pageNumber - 1); // 1-based to 0-based
    }
  };

  const handlePageSizeChange = (newPageSize: string) => {
    const size = Number.parseInt(newPageSize);
    if (gridApi) {
      gridApi.setGridOption("paginationPageSize", size);
      setPageSize(size);
    }
  };

  // 페이지 번호 버튼 생성
  const renderPageNumbers = () => {
    const pages = [];
    const maxPagesToShow = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxPagesToShow / 2));
    const endPage = Math.min(totalPages, startPage + maxPagesToShow - 1);

    if (endPage - startPage < maxPagesToShow - 1) {
      startPage = Math.max(1, endPage - maxPagesToShow + 1);
    }

    // 첫 페이지
    if (startPage > 1) {
      pages.push(
        <Button
          key={1}
          variant="ghost"
          size="sm"
          onClick={() => goToPage(1)}
          className="h-8 w-8 p-0"
        >
          1
        </Button>
      );
      if (startPage > 2) {
        pages.push(
          <span key="ellipsis-start" className="px-1">
            ...
          </span>
        );
      }
    }

    // 중간 페이지들
    for (let i = startPage; i <= endPage; i++) {
      pages.push(
        <Button
          key={i}
          variant={currentPage === i ? "default" : "ghost"}
          size="sm"
          onClick={() => goToPage(i)}
          className="h-8 w-8 p-0"
        >
          {i}
        </Button>
      );
    }

    // 마지막 페이지
    if (endPage < totalPages) {
      if (endPage < totalPages - 1) {
        pages.push(
          <span key="ellipsis-end" className="px-1">
            ...
          </span>
        );
      }
      pages.push(
        <Button
          key={totalPages}
          variant="ghost"
          size="sm"
          onClick={() => goToPage(totalPages)}
          className="h-8 w-8 p-0"
        >
          {totalPages}
        </Button>
      );
    }

    return pages;
  };

  if (!gridApi) return null;

  return (
    <div
      className={cn("flex items-center justify-between bg-white", className)}
    >
      {/* 왼쪽: 행 정보 */}
      <div className="flex items-center gap-4 text-sm text-gray-600">
        <span>
          {displayedRows.start}-{displayedRows.end} / 전체 {totalRows}개
        </span>
        <div className="flex items-center gap-2">
          <span>페이지당 행 수:</span>
          <Select
            value={pageSize.toString()}
            onValueChange={handlePageSizeChange}
          >
            <SelectTrigger className="h-8 w-[70px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {pageSizeOptions.map((size) => (
                <SelectItem key={size} value={size.toString()}>
                  {size}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {/* 오른쪽: 페이지 네비게이션 */}
      <div className="flex items-center gap-2">
        <Button
          variant="ghost"
          size="sm"
          onClick={goToFirstPage}
          disabled={currentPage === 1}
          className="h-8 w-8 p-0"
        >
          <ChevronsLeft className="h-4 w-4" />
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={goToPreviousPage}
          disabled={currentPage === 1}
          className="h-8 w-8 p-0"
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>

        <div className="flex items-center gap-1">{renderPageNumbers()}</div>

        <Button
          variant="ghost"
          size="sm"
          onClick={goToNextPage}
          disabled={currentPage === totalPages}
          className="h-8 w-8 p-0"
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
        <Button
          variant="ghost"
          size="sm"
          onClick={goToLastPage}
          disabled={currentPage === totalPages}
          className="h-8 w-8 p-0"
        >
          <ChevronsRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
