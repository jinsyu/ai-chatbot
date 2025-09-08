import type { Metadata } from "next";
import BiPageUI from "./ui";

export const metadata: Metadata = {
  title: `매출 개요 및 부서별 매출 실적 | ${process.env.NEXT_PUBLIC_SITE_NAME}`,
  description: "매출 개요 및 부서별 매출 실적을 확인할 수 있습니다.",
};

export default function BiPage() {
  return <BiPageUI />;
}
