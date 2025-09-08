"use client";
import { Button } from "@/components/ui/button";
import { ChevronLeft } from "lucide-react";

export default function NotFound() {
  return (
    <div className="flex flex-col  min-h-screen">
      <div className="p-8 flex flex-col bg-white">
        <div className="text-2xl font-bold mb-1">404 Not Found</div>
        <div className="text-lg mb-6 leading-relaxed">
          현재 해당 페이지는 개발 중입니다.
        </div>
        <div className="flex gap-4">
          <Button variant="outline" onClick={() => window.history.back()}>
            <ChevronLeft className="w-4 h-4" />
            이전 화면으로
          </Button>
        </div>
      </div>
    </div>
  );
}
