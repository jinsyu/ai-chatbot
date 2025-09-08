import { NextResponse } from 'next/server';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';

// Create database connection
const client = postgres(process.env.POSTGRES_URL!);
const db = drizzle(client);

export async function GET() {
  try {
    // 1. 부족수량 상세 분석
    const shortageDetailResult = await db.execute(sql`
      SELECT 
        o."판매오더",
        o."고객명_판매처",
        o."자재",
        o."자재내역",
        o."제품군명",
        o."자재그룹5명",
        CAST(o."오더수량" AS INTEGER) as order_qty,
        CAST(o."가용재고" AS INTEGER) as available_stock,
        CAST(o."납품가능수량" AS INTEGER) as deliverable_qty,
        CAST(o."오더수량" AS INTEGER) - CAST(o."납품가능수량" AS INTEGER) as shortage_qty,
        CAST(o."납품가능금액" AS DECIMAL) as deliverable_amount,
        TO_DATE(o."납기일_품목", 'YYYY-MM-DD') as delivery_date,
        o."납품우선순위" as priority,
        o."생산예정수량",
        o."생산예정일자",
        o."생산사유",
        CASE 
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE THEN 'Overdue'
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '3 days' THEN 'Critical'
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '7 days' THEN 'High'
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '14 days' THEN 'Medium'
          ELSE 'Low'
        END as urgency
      FROM sap_zsdr0062_sales_orders o
      WHERE CAST(o."오더수량" AS INTEGER) > CAST(o."납품가능수량" AS INTEGER)
      ORDER BY 
        CASE 
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE THEN 1
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '3 days' THEN 2
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '7 days' THEN 3
          WHEN TO_DATE(o."납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '14 days' THEN 4
          ELSE 5
        END,
        shortage_qty DESC
      LIMIT 100
    `);

    // 2. 긴급도별 집계
    const urgencySummaryResult = await db.execute(sql`
      SELECT 
        CASE 
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE THEN 'Overdue'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '3 days' THEN 'Critical'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '7 days' THEN 'High'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '14 days' THEN 'Medium'
          ELSE 'Low'
        END as urgency,
        COUNT(*) as order_count,
        SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER)) as total_shortage,
        SUM(CAST("공급금액" AS DECIMAL) - CAST("납품가능금액" AS DECIMAL)) as shortage_amount
      FROM sap_zsdr0062_sales_orders
      WHERE CAST("오더수량" AS INTEGER) > CAST("납품가능수량" AS INTEGER)
      GROUP BY 
        CASE 
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE THEN 'Overdue'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '3 days' THEN 'Critical'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '7 days' THEN 'High'
          WHEN TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '14 days' THEN 'Medium'
          ELSE 'Low'
        END
    `);

    // 3. 고객별 부족수량 집계
    const customerShortageResult = await db.execute(sql`
      SELECT 
        "고객명_판매처" as customer,
        COUNT(*) as order_count,
        SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER)) as total_shortage,
        SUM(CAST("공급금액" AS DECIMAL)) as total_order_amount,
        SUM(CAST("납품가능금액" AS DECIMAL)) as deliverable_amount,
        MIN(TO_DATE("납기일_품목", 'YYYY-MM-DD')) as earliest_delivery
      FROM sap_zsdr0062_sales_orders
      WHERE CAST("오더수량" AS INTEGER) > CAST("납품가능수량" AS INTEGER)
      GROUP BY "고객명_판매처"
      ORDER BY total_shortage DESC
      LIMIT 20
    `);

    // 4. 제품별 부족수량 분석
    const productShortageResult = await db.execute(sql`
      SELECT 
        o."자재",
        o."자재내역",
        o."제품군명",
        COUNT(DISTINCT o."판매오더") as affected_orders,
        SUM(CAST(o."오더수량" AS INTEGER) - CAST(o."납품가능수량" AS INTEGER)) as total_shortage,
        SUM(CAST(o."가용재고" AS INTEGER)) as total_available,
        i."6개월평균판매수량" as avg_sales_qty,
        i."재고금액" as inventory_value,
        m."판가" as selling_price
      FROM sap_zsdr0062_sales_orders o
      LEFT JOIN sap_zmmr0016_inventory i ON o."자재" = i."자재"
      LEFT JOIN sap_zmmr0001_materials m ON o."자재" = m."자재"
      WHERE CAST(o."오더수량" AS INTEGER) > CAST(o."납품가능수량" AS INTEGER)
      GROUP BY o."자재", o."자재내역", o."제품군명", i."6개월평균판매수량", i."재고금액", m."판가"
      ORDER BY total_shortage DESC
      LIMIT 30
    `);

    // 5. 생산 예정 vs 부족수량
    const productionPlanResult = await db.execute(sql`
      SELECT 
        "자재",
        "자재내역",
        SUM(CAST("오더수량" AS INTEGER)) as total_order,
        SUM(CAST("납품가능수량" AS INTEGER)) as total_deliverable,
        SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER)) as shortage,
        SUM(CAST("생산예정수량" AS INTEGER)) as planned_production,
        MAX("생산예정일자") as production_date,
        CASE 
          WHEN SUM(CAST("생산예정수량" AS INTEGER)) >= SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER))
          THEN '충족'
          ELSE '부족'
        END as production_status
      FROM sap_zsdr0062_sales_orders
      WHERE CAST("오더수량" AS INTEGER) > CAST("납품가능수량" AS INTEGER)
        AND "생산예정수량" IS NOT NULL
      GROUP BY "자재", "자재내역"
      ORDER BY shortage DESC
    `);

    // 6. 납기일별 부족수량 트렌드
    const deliveryTrendResult = await db.execute(sql`
      SELECT 
        TO_DATE("납기일_품목", 'YYYY-MM-DD') as delivery_date,
        COUNT(*) as order_count,
        SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER)) as shortage_qty,
        SUM(CAST("공급금액" AS DECIMAL) - CAST("납품가능금액" AS DECIMAL)) as shortage_amount
      FROM sap_zsdr0062_sales_orders
      WHERE CAST("오더수량" AS INTEGER) > CAST("납품가능수량" AS INTEGER)
        AND TO_DATE("납기일_품목", 'YYYY-MM-DD') >= CURRENT_DATE
        AND TO_DATE("납기일_품목", 'YYYY-MM-DD') <= CURRENT_DATE + INTERVAL '30 days'
      GROUP BY TO_DATE("납기일_품목", 'YYYY-MM-DD')
      ORDER BY delivery_date
    `);

    // 7. 대체품 추천 (동일 제품군 내 재고 충분한 제품)
    const alternativeProductsResult = await db.execute(sql`
      WITH shortage_products AS (
        SELECT DISTINCT 
          o."자재",
          o."제품군명",
          o."자재그룹5명",
          SUM(CAST(o."오더수량" AS INTEGER) - CAST(o."납품가능수량" AS INTEGER)) as shortage_qty
        FROM sap_zsdr0062_sales_orders o
        WHERE CAST(o."오더수량" AS INTEGER) > CAST(o."납품가능수량" AS INTEGER)
        GROUP BY o."자재", o."제품군명", o."자재그룹5명"
      )
      SELECT 
        sp."자재" as shortage_material,
        sp.shortage_qty,
        i."자재" as alternative_material,
        i."자재명" as alternative_name,
        CAST(i."가용재고수량" AS INTEGER) as available_qty,
        CAST(i."재고금액" AS DECIMAL) as inventory_value,
        m."판가" as selling_price
      FROM shortage_products sp
      JOIN sap_zmmr0016_inventory i ON i."제품군명" = sp."제품군명"
      LEFT JOIN sap_zmmr0001_materials m ON i."자재" = m."자재"
      WHERE i."자재" != sp."자재"
        AND CAST(i."가용재고수량" AS INTEGER) > sp.shortage_qty
      ORDER BY sp.shortage_qty DESC, i."가용재고수량" DESC
      LIMIT 20
    `);

    // 8. CY창고 재고 활용 가능성
    const cyStockUtilizationResult = await db.execute(sql`
      SELECT 
        "자재",
        "자재내역",
        SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER)) as shortage_qty,
        SUM(CAST("CY창고수량" AS INTEGER)) as cy_stock,
        CASE 
          WHEN SUM(CAST("CY창고수량" AS INTEGER)) >= SUM(CAST("오더수량" AS INTEGER) - CAST("납품가능수량" AS INTEGER))
          THEN '즉시해결가능'
          WHEN SUM(CAST("CY창고수량" AS INTEGER)) > 0
          THEN '부분해결가능'
          ELSE '해결불가'
        END as solution_status
      FROM sap_zsdr0062_sales_orders
      WHERE CAST("오더수량" AS INTEGER) > CAST("납품가능수량" AS INTEGER)
      GROUP BY "자재", "자재내역"
      HAVING SUM(CAST("CY창고수량" AS INTEGER)) > 0
      ORDER BY shortage_qty DESC
    `);

    return NextResponse.json({
      shortageDetail: shortageDetailResult,
      urgencySummary: urgencySummaryResult,
      customerShortage: customerShortageResult,
      productShortage: productShortageResult,
      productionPlan: productionPlanResult,
      deliveryTrend: deliveryTrendResult,
      alternativeProducts: alternativeProductsResult,
      cyStockUtilization: cyStockUtilizationResult,
    });
  } catch (error) {
    console.error('Shortage analysis error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch shortage data' },
      { status: 500 },
    );
  }
}