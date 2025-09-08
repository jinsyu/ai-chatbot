"use client";

import type { User } from "next-auth";
import { useRouter, usePathname } from "next/navigation";

import { PlusIcon } from "@/components/icons";
import { SidebarHistory } from "@/components/sidebar-history";
import { SidebarUserNav } from "@/components/sidebar-user-nav";
import { Button } from "@/components/ui/button";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarGroupContent,
  useSidebar,
} from "@/components/ui/sidebar";
import Link from "next/link";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";
import { MessageSquare } from "lucide-react";

export function AppSidebar({ user }: { user: User | undefined }) {
  const router = useRouter();
  const pathname = usePathname();
  const { setOpenMobile } = useSidebar();

  const salesDashboardMenu = [
    {
      title: "매출 개요",
      url: "/reports/sales-overview",
    },
    {
      title: "부서별 매출 실적",
      url: "/reports/sales-department",
    },
    {
      title: "월별 목표/실적 개요",
      url: "/reports/sales-monthly-overview",
    },
    {
      title: "부서별 월별 목표/실적",
      url: "/reports/sales-monthly-department",
    },
    {
      title: "고객별 매출 실적",
      url: "/reports/sales-monthly-customer",
    },
    {
      title: "제품별 매출 실적",
      url: "/reports/sales-monthly-product",
    },
    {
      title: "종합 매트릭스",
      url: "/reports/sales-total-matrix",
    },
  ];

  const inventoryDashboardMenu = [
    {
      title: "재고 개요",
      url: "/reports/inventory-overview",
    },
    {
      title: "월별 재고 목표/실적",
      url: "/reports/inventory-monthly",
    },
    {
      title: "자재그룹별 재고 실적",
      url: "/reports/inventory-product-group",
    },
    {
      title: "부족 재고 현황",
      url: "/reports/inventory-shortage",
    },
  ];

  return (
    <Sidebar className="group-data-[side=left]:border-r-0">
      <SidebarHeader>
        <SidebarMenu>
          <div className="flex flex-row justify-between items-center">
            <Link
              href="/"
              onClick={() => {
                setOpenMobile(false);
              }}
              className="flex flex-row gap-3 items-center"
            >
              <span className="text-lg font-semibold px-2 hover:bg-muted rounded-md cursor-pointer">
                SAP assistant
              </span>
            </Link>
            {pathname?.startsWith("/chat") || pathname === "/" ? (
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    type="button"
                    className="p-2 h-fit"
                    onClick={() => {
                      setOpenMobile(false);
                      router.push("/");
                      router.refresh();
                    }}
                  >
                    <PlusIcon />
                  </Button>
                </TooltipTrigger>
                <TooltipContent align="end">새 대화</TooltipContent>
              </Tooltip>
            ) : null}
          </div>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        {/* 대시보드 메뉴 섹션 */}
        <SidebarGroup>
          <SidebarGroupLabel>매출현황 대시보드</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {salesDashboardMenu.map((menu) => (
                <SidebarMenuItem key={menu.url}>
                  <SidebarMenuButton asChild isActive={pathname === menu.url}>
                    <Link href={menu.url} onClick={() => setOpenMobile(false)}>
                      <span>{menu.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel>재고현황 대시보드</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {inventoryDashboardMenu.map((menu) => (
                <SidebarMenuItem key={menu.url}>
                  <SidebarMenuButton asChild isActive={pathname === menu.url}>
                    <Link href={menu.url} onClick={() => setOpenMobile(false)}>
                      <span>{menu.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        {/* AI 챗봇 섹션 */}
        <SidebarGroup>
          <SidebarGroupLabel>AI 챗봇</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton
                  asChild
                  isActive={pathname === "/" || pathname?.startsWith("/chat")}
                >
                  <Link href="/" onClick={() => setOpenMobile(false)}>
                    <MessageSquare className="w-4 h-4" />
                    <span>새 대화</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
            <div className="mt-2">
              <SidebarHistory user={user} />
            </div>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>{user && <SidebarUserNav user={user} />}</SidebarFooter>
    </Sidebar>
  );
}
