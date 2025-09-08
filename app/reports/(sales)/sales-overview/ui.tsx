"use client";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import CountUp from "react-countup";
import { CheckCircle2, AlertTriangle, XCircle } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import MonthlySalesChart from "@/components/charts/MonthlySalesChart";

const kpiData = [
  { label: "총 매출목표", value: 5603654747 },
  { label: "총 매출실적", value: 5125922216 },
  { label: "목표 대비 증감액", value: -477732531 },
  { label: "목표달성률", value: 92, suffix: "%", isSmall: true },
  { label: "증감율(전년)", value: 42, prefix: "+", suffix: "%", isSmall: true },
];

const departmentSales = [
  {
    name: "국내영업",
    매출실적전년동월: 2465633497,
    매출실적당월: 2715573056,
    매출목표당월: 2715573056,
    달성률: "110%",
    증감율: "+39%",
    비고: "ok",
  },
  {
    name: "조달",
    매출실적전년동월: 1475115000,
    매출실적당월: 857979675,
    매출목표당월: 857979675,
    달성률: "58%",
    증감율: "+37%",
    비고: "warn",
  },
  {
    name: "해외영업",
    매출실적전년동월: 1063962056,
    매출실적당월: 758461155,
    매출목표당월: 758461155,
    달성률: "-71%",
    증감율: "-29%",
    비고: "fail",
  },
  {
    name: "철도사업",
    매출실적전년동월: 489000000,
    매출실적당월: 620000,
    매출목표당월: 620000,
    달성률: "127%",
    증감율: "+109%",
    비고: "ok",
  },
  {
    name: "SI",
    매출실적전년동월: 489000000,
    매출실적당월: 620000,
    매출목표당월: 620000,
    달성률: "127%",
    증감율: "+109%",
    비고: "ok",
  },
  {
    name: "인터랙트",
    매출실적전년동월: 489000000,
    매출실적당월: 620000,
    매출목표당월: 620000,
    달성률: "127%",
    증감율: "+109%",
    비고: "ok",
  },
];
const productGroupSales = [
  {
    name: "PA",
    매출실적전년동월: 3953827651,
    매출목표당월: 4349301370,
    매출실적당월: 4349301370,
    달성률: "91%",
    증감율: "+10%",
    비고: "ok",
  },
  {
    name: "SR",
    매출실적전년동월: 1057603763,
    매출목표당월: 1163364099,
    매출실적당월: 1163364099,
    달성률: "91%",
    증감율: "+8%",
    비고: "ok",
  },
  {
    name: "AV",
    매출실적전년동월: 3281321755,
    매출목표당월: 361153931,
    매출실적당월: 361153931,
    달성률: "91%",
    증감율: "+7%",
    비고: "warn",
  },
  {
    name: "통신",
    매출실적전년동월: 316219519,
    매출실적당월: 347885110,
    매출목표당월: 347885110,
    달성률: "91%",
    증감율: "+5%",
    비고: "fail",
  },
];

const productTypeSales = [
  {
    name: "소스",
    매출실적전년동월: 757164099,
    매출목표당월: 832880509,
    매출실적당월: 832880509,
    달성률: "91%",
    증감율: "+8%",
    비고: "ok",
  },
  {
    name: "컨트롤러",
    매출실적전년동월: 1057603763,
    매출실적당월: 1163364099,
    매출목표당월: 1163364099,
    달성률: "91%",
    증감율: "+8%",
    비고: "ok",
  },
  {
    name: "앰프",
    매출실적전년동월: 3281321755,
    매출실적당월: 361153931,
    매출목표당월: 361153931,
    달성률: "91%",
    증감율: "+7%",
    비고: "warn",
  },
  {
    name: "스피커",
    매출실적전년동월: 316219519,
    매출실적당월: 347885110,
    매출목표당월: 347885110,
    달성률: "91%",
    증감율: "+5%",
    비고: "fail",
  },
  {
    name: "공통",
    매출실적전년동월: 316219519,
    매출실적당월: 347885110,
    매출목표당월: 347885110,
    달성률: "91%",
    증감율: "+5%",
    비고: "fail",
  },
];

function KpiSummary() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-6">
      {kpiData.map((kpi) => (
        <Card
          key={kpi.label}
          className="border border-gray-200 shadow-none min-w-0 flex flex-col items-center"
        >
          <CardHeader className="w-full flex flex-col items-center">
            <CardDescription className="text-base font-semibold text-gray-800 text-center tracking-tight">
              {kpi.label}
            </CardDescription>
            <CardTitle className={`flex flex-col items-center w-full`}>
              <span
                className={`block w-full ${
                  kpi.isSmall
                    ? "text-2xl font-bold text-gray-700"
                    : "text-2xl font-extrabold text-gray-900"
                } leading-tight text-center`}
              >
                {typeof kpi.value === "number" && !kpi.isSmall ? (
                  <>
                    <CountUp
                      end={Math.abs(kpi.value)}
                      duration={1.2}
                      separator=","
                      prefix={kpi.prefix || (kpi.value < 0 ? "-" : "")}
                      suffix={kpi.suffix || ""}
                      className={
                        kpi.value > 0 ? "text-gray-900" : "text-red-500"
                      }
                    />
                  </>
                ) : (
                  <CountUp
                    end={Math.abs(kpi.value)}
                    duration={1.2}
                    separator=","
                    prefix={kpi.prefix || (kpi.value < 0 ? "-" : "")}
                    suffix={kpi.suffix || ""}
                  />
                )}
              </span>
            </CardTitle>
          </CardHeader>
        </Card>
      ))}
    </div>
  );
}

function DepartmentSalesTable() {
  return (
    <Card className="border border-gray-200">
      <CardHeader>
        <CardTitle className="text-lg font-bold text-gray-900">
          부서별 매출 현황
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow className="bg-gray-100">
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                구분
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(전년동월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출목표(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                달성률
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                증감율(전년)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                비고
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {departmentSales.map((row) => (
              <TableRow key={row.name}>
                <TableCell className="whitespace-nowrap">{row.name}</TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적전년동월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적전년동월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출목표당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출목표당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell
                  className={cn(
                    Number(row.달성률.replace("%", "").trim()) > 100
                      ? "text-blue-700"
                      : "text-red-700"
                  )}
                >
                  {row.달성률}
                </TableCell>
                <TableCell
                  className={cn(
                    Number(row.증감율.replace("%", "").trim()) > 0
                      ? "text-blue-700"
                      : "text-red-700"
                  )}
                >
                  {row.증감율}
                </TableCell>
                <TableCell>
                  {row.비고 === "ok" && (
                    <CheckCircle2 className="text-green-500 size-4" />
                  )}
                  {row.비고 === "warn" && (
                    <AlertTriangle className="text-yellow-500 size-4" />
                  )}
                  {row.비고 === "fail" && (
                    <XCircle className="text-red-500 size-4" />
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
          <TableFooter>
            <TableRow>
              <TableCell className="whitespace-nowrap">합계</TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출목표당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    departmentSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell>
                <span className="text-blue-700">100%</span>
              </TableCell>
              <TableCell>
                <span className="text-blue-700">+10%</span>
              </TableCell>
              <TableCell>
                <span className="text-green-500">
                  <CheckCircle2 className="text-green-500 size-4" />
                </span>
              </TableCell>
            </TableRow>
          </TableFooter>
        </Table>
      </CardContent>
    </Card>
  );
}

function ProductGroupSalesTable() {
  return (
    <Card className="border border-gray-200">
      <CardHeader>
        <CardTitle className="text-lg font-bold text-gray-900">
          제품군별 매출 현황
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow className="bg-gray-100">
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                구분
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(전년동월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출목표(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                달성률
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                증감율(전년)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                비고
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {productGroupSales.map((row) => (
              <TableRow key={row.name}>
                <TableCell className="whitespace-nowrap">{row.name}</TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적전년동월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적전년동월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출목표당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출목표당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell>
                  <span
                    className={cn(
                      Number(row.달성률.replace("%", "").trim()) > 100
                        ? "text-blue-700"
                        : "text-red-700"
                    )}
                  >
                    {row.달성률}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    className={cn(
                      Number(row.증감율.replace("%", "").trim()) > 0
                        ? "text-blue-700"
                        : "text-red-700"
                    )}
                  >
                    {row.증감율}
                  </span>
                </TableCell>
                <TableCell>
                  {row.비고 === "ok" && (
                    <CheckCircle2 className="text-green-500 size-4" />
                  )}
                  {row.비고 === "warn" && (
                    <AlertTriangle className="text-yellow-500 size-4" />
                  )}
                  {row.비고 === "fail" && (
                    <XCircle className="text-red-500 size-4" />
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
          <TableFooter>
            <TableRow>
              <TableCell className="whitespace-nowrap">합계</TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출목표당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productGroupSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell>
                <span className="text-blue-700">100%</span>
              </TableCell>
              <TableCell>
                <span className="text-blue-700">+10%</span>
              </TableCell>
              <TableCell>
                <span className="text-green-500">
                  <CheckCircle2 className="text-green-500 size-4" />
                </span>
              </TableCell>
            </TableRow>
          </TableFooter>
        </Table>
      </CardContent>
    </Card>
  );
}

function ProductTypeSalesTable() {
  return (
    <Card className="border border-gray-200">
      <CardHeader>
        <CardTitle className="text-lg font-bold text-gray-900">
          제품유형별 매출 현황
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow className="bg-gray-100">
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                구분
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(전년동월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출실적(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                매출목표(당월)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                달성률
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                증감(전년)
              </TableHead>
              <TableHead className="text-gray-700 font-semibold whitespace-nowrap">
                비고
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {productTypeSales.map((row) => (
              <TableRow key={row.name}>
                <TableCell className="whitespace-nowrap">{row.name}</TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적전년동월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적전년동월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출목표당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출목표당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell className="text-right">
                  <CountUp
                    end={Math.abs(row.매출실적당월)}
                    duration={1}
                    separator=","
                    prefix={row.매출실적당월 < 0 ? "-" : ""}
                  />
                </TableCell>
                <TableCell>
                  <span
                    className={cn(
                      Number(row.달성률.replace("%", "").trim()) > 100
                        ? "text-blue-700"
                        : "text-red-700"
                    )}
                  >
                    {row.달성률}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    className={cn(
                      Number(row.증감율.replace("%", "").trim()) > 0
                        ? "text-blue-700"
                        : "text-red-700"
                    )}
                  >
                    {row.증감율}
                  </span>
                </TableCell>
                <TableCell>
                  {row.비고 === "ok" && (
                    <CheckCircle2 className="text-green-500 size-4" />
                  )}
                  {row.비고 === "warn" && (
                    <AlertTriangle className="text-yellow-500 size-4" />
                  )}
                  {row.비고 === "fail" && (
                    <XCircle className="text-red-500 size-4" />
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
          <TableFooter>
            <TableRow>
              <TableCell>합계</TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출실적전년동월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출목표당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell className="text-right">
                <CountUp
                  end={Math.abs(
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    )
                  )}
                  duration={1}
                  separator=","
                  prefix={
                    productTypeSales.reduce(
                      (acc, row) => acc + row.매출실적당월,
                      0
                    ) < 0
                      ? "-"
                      : ""
                  }
                />
              </TableCell>
              <TableCell>
                <span className="text-blue-700">100%</span>
              </TableCell>
              <TableCell>
                <span className="text-blue-700">+10%</span>
              </TableCell>
              <TableCell>
                <span className="text-green-500">
                  <CheckCircle2 className="text-green-500 size-4" />
                </span>
              </TableCell>
            </TableRow>
          </TableFooter>
        </Table>
      </CardContent>
    </Card>
  );
}

export default function BiPageUI() {
  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h1 className="flex items-center gap-4">
          <span className="text-2xl font-bold ">매출 개요(Overview)</span>
          <span className="text-gray-500">2025.03 ~ 2025.03</span>
        </h1>
        <Select>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="기간선택" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="2025.03">2025.03</SelectItem>
            <SelectItem value="2025.04">2025.04</SelectItem>
            <SelectItem value="2025.05">2025.05</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <KpiSummary />
      <div className="grid grid-cols-1 2xl:grid-cols-2 gap-4">
        <MonthlySalesChart />
        <DepartmentSalesTable />
      </div>
      <div className="grid grid-cols-1 2xl:grid-cols-2 gap-4">
        <ProductGroupSalesTable />
        <ProductTypeSalesTable />
      </div>
    </div>
  );
}
