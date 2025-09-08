import type { Metadata } from "next";
import MonthlyPageUI from "./ui";

export const metadata: Metadata = {
  title: `월별 목표/실적 개요 | ${process.env.NEXT_PUBLIC_SITE_NAME}`,
  description: "월별 목표/실적 개요를 확인할 수 있습니다.",
};

export default function MonthlyPage() {
  return <MonthlyPageUI />;
}
