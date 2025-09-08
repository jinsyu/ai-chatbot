"use client";

import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import {
  Bar,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Line,
  LabelList,
  CartesianGrid,
  ComposedChart,
} from "recharts";

const monthlySales = [
  { month: "2024.10", 목표: 3208, 실적: 3115, 전년: 3208 },
  { month: "2024.11", 목표: 4287, 실적: 3588, 전년: 3115 },
  { month: "2024.12", 목표: 5905, 실적: 5421, 전년: 4287 },
  { month: "2025.01", 목표: 4417, 실적: 3425, 전년: 5905 },
  { month: "2025.02", 목표: 5192, 실적: 3479, 전년: 4417 },
  { month: "2025.03", 목표: 5604, 실적: 5126, 전년: 5192 },
];

export default function MonthlySalesChart() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>월별 매출 추이</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <ComposedChart
            data={monthlySales}
            margin={{ top: 0, right: 0, left: 0, bottom: 0 }}
            barGap={0}
            barCategoryGap="20%"
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" stroke="#888" fontSize={16} />
            <YAxis stroke="#888" fontSize={16} />
            <Tooltip />
            <Legend
              verticalAlign="top"
              align="right"
              iconType="rect"
              wrapperStyle={{ top: 0, right: 0 }}
              payload={[
                { value: "매출목표", type: "rect", color: "#2563eb" },
                { value: "매출실적", type: "rect", color: "#60a5fa" },
                { value: "전년실적", type: "line", color: "#bbb" },
              ]}
            />
            <Line
              type="monotone"
              dataKey="전년"
              name="전년실적"
              stroke="#bbb"
              strokeWidth={2}
              dot={{ r: 5, fill: "#bbb", stroke: "#fff", strokeWidth: 1 }}
            />
            <Bar
              dataKey="목표"
              name="매출목표"
              fill="#2563eb"
              barSize={40}
              radius={[4, 4, 0, 0]}
              opacity={0.85}
            >
              <LabelList
                dataKey="목표"
                position="top"
                fill="#222"
                fontWeight={500}
                fontSize={10}
              />
            </Bar>
            <Bar
              dataKey="실적"
              name="매출실적"
              fill="#60a5fa"
              barSize={40}
              radius={[4, 4, 0, 0]}
              opacity={0.85}
            >
              <LabelList
                dataKey="실적"
                position="top"
                fill="#222"
                fontWeight={500}
                fontSize={10}
              />
            </Bar>
          </ComposedChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}
