import type { Metadata } from "next";
import BiPageUI from "./ui";

export const metadata: Metadata = {
  title: `월별 재고 목표 및 실적 | ${process.env.NEXT_PUBLIC_SITE_NAME}`,
  description: "월별 재고 목표 및 실적을 확인할 수 있습니다.",
};

export default function BiPage() {
  return <BiPageUI />;
}
