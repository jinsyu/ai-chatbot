import { NextResponse } from 'next/server';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';

// Create database connection
const client = postgres(process.env.POSTGRES_URL!);
const db = drizzle(client);

export async function GET() {
  try {
    // 1. 총 재고금액 및 수량 (sap_zmmr0016_inventory)
    const inventoryTotalResult = await db.execute(sql`
      SELECT 
        COALESCE(SUM(CAST("재고금액" AS DECIMAL)), 0) as total_inventory_value,
        COALESCE(SUM(CAST("총재고수량" AS DECIMAL)), 0) as total_inventory_quantity
      FROM sap_zmmr0016_inventory
    `);

    console.log('inventoryTotalResult', inventoryTotalResult);

    // 2. 이번달 매출액 (sap_zsdr0340_sales_detail)
    const currentMonthSalesResult = await db.execute(sql`
      SELECT COALESCE(SUM(CAST("청구금액" AS DECIMAL)), 0) as current_month_sales
      FROM sap_zsdr0340_sales_detail
      WHERE DATE_TRUNC('month', TO_DATE("청구일", 'YYYY-MM-DD')) = DATE_TRUNC('month', CURRENT_DATE)
    `);

    // 3. 부족수량 알림 (상위 5개)
    const shortageResult = await db.execute(sql`
      WITH shortage_materials AS (
        SELECT 
          o."자재",
          o."자재내역",
          SUM(CAST(o."오더수량" AS INTEGER) - CAST(o."납품가능수량" AS INTEGER)) as shortage_qty,
          COUNT(*) as order_count,
          AVG(CAST(o."오더수량" AS INTEGER)) as avg_order_qty
        FROM sap_zsdr0062_sales_orders o
        WHERE CAST(o."오더수량" AS INTEGER) > CAST(o."납품가능수량" AS INTEGER)
        GROUP BY o."자재", o."자재내역"
        ORDER BY shortage_qty DESC
        LIMIT 5
      )
      SELECT 
        s.*,
        COALESCE(i."가용재고수량", 0) as available_stock,
        COALESCE(i."재고금액", 0) as stock_value,
        COALESCE(m."판매단위", 0) as unit,
        COALESCE(m."자재그룹7명", '') as product_group
      FROM shortage_materials s
      LEFT JOIN sap_zmmr0016_inventory i ON s."자재" = i."자재"
      LEFT JOIN sap_zmmr0001_materials m ON s."자재" = m."자재"
      ORDER BY s.shortage_qty DESC
    `);

    // 4. 활성 자재 수
    const activeMaterialsResult = await db.execute(sql`
      SELECT COUNT(DISTINCT "자재") as active_materials
      FROM sap_zmmr0001_materials
      WHERE "단종여부" IS NULL OR "단종여부" != '1'
    `);

    // 5. 2025년 전체 매출 추이
    const monthlySalesResult = await db.execute(sql`
      SELECT 
        TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-MM') as month,
        COALESCE(SUM(CAST("청구금액" AS DECIMAL)), 0) as sales
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= '2025-01-01'
      GROUP BY TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-MM')
      ORDER BY month
    `);

    // 6. 제품군별 매출 (자재그룹7명 기준) - 필수
    const salesByProductGroupResult = await db.execute(sql`
      SELECT 
        COALESCE(NULLIF(TRIM("자재그룹7명"), ''), '기타') as product_group,
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        COUNT(DISTINCT "자재") as material_count,
        COUNT(*) as transaction_count
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY COALESCE(NULLIF(TRIM("자재그룹7명"), ''), '기타')
      HAVING SUM(CAST("청구금액" AS DECIMAL)) > 0
      ORDER BY total_sales DESC
    `);

    // 7. 제품유형별 매출 (자재그룹6명 기준) - 필수
    const salesByProductTypeResult = await db.execute(sql`
      SELECT 
        COALESCE("자재그룹6명", '기타') as product_type,
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        COUNT(DISTINCT "자재") as material_count
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
        AND "자재그룹6명" IS NOT NULL
      GROUP BY COALESCE("자재그룹6명", '기타')
      ORDER BY total_sales DESC
      LIMIT 10
    `);

    // 8. 재고상태별 금액 (재고구분명 기준) - 필수
    const inventoryByStatusResult = await db.execute(sql`
      SELECT 
        COALESCE("재고구분명", '기타') as inventory_status,
        SUM(CAST("재고금액" AS DECIMAL)) as total_value,
        SUM(CAST("총재고수량" AS DECIMAL)) as total_quantity,
        COUNT(DISTINCT "자재") as material_count
      FROM sap_zmmr0016_inventory
      WHERE "재고구분명" IS NOT NULL
      GROUP BY COALESCE("재고구분명", '기타')
      ORDER BY total_value DESC
    `);

    // 9. 재고 회전율 분석
    const inventoryTurnoverResult = await db.execute(sql`
      SELECT 
        AVG(CASE 
          WHEN CAST("재고금액" AS DECIMAL) > 0 
          THEN CAST("6개월평균판매금액" AS DECIMAL) * 2 / CAST("재고금액" AS DECIMAL)
          ELSE 0 
        END) as avg_turnover_rate,
        COUNT(CASE WHEN CAST("6개월평균판매수량" AS DECIMAL) = 0 OR "6개월평균판매수량" IS NULL THEN 1 END) as slow_moving_count,
        COUNT(CASE WHEN CAST("MT24M(금액)" AS DECIMAL) > 0 THEN 1 END) as obsolete_count
      FROM sap_zmmr0016_inventory
    `);

    // 10. ABC 재고 분석 - 실제 등급 재고만
    const abcInventoryResult = await db.execute(sql`
      SELECT 
        COALESCE(SUM(CAST("가용재고금액" AS DECIMAL)), 0) as a_grade_value,
        COALESCE(SUM(CAST("B등급" AS DECIMAL)), 0) as b_grade_value,
        COALESCE(SUM(CAST("C등급" AS DECIMAL)), 0) as c_grade_value,
        COALESCE(SUM(CAST("D등급" AS DECIMAL)), 0) as d_grade_value,
        COALESCE(SUM(CAST("등급재고금액" AS DECIMAL)), 0) as total_grade_value
      FROM sap_zmmr0016_inventory
    `);

    // 11. 오더 달성률
    const orderFulfillmentResult = await db.execute(sql`
      SELECT 
        AVG(CASE 
          WHEN CAST("오더수량" AS DECIMAL) > 0 
          THEN CAST("출고수량" AS DECIMAL) / CAST("오더수량" AS DECIMAL) * 100
          ELSE 0 
        END) as fulfillment_rate,
        COUNT(*) as total_orders,
        COUNT(CASE WHEN CAST("출고수량" AS DECIMAL) >= CAST("오더수량" AS DECIMAL) THEN 1 END) as completed_orders
      FROM sap_zsdr0062_sales_orders
    `);

    // 12. 고객별 매출 (판매처명 기준) - 필수
    const topCustomersResult = await db.execute(sql`
      SELECT 
        "판매처명",
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        COUNT(DISTINCT "대금청구문서") as invoice_count
      FROM sap_zsdr0340_sales_detail
      WHERE "판매처명" IS NOT NULL
      GROUP BY "판매처명"
      ORDER BY total_sales DESC
      LIMIT 10
    `);

    // 13. 재고 추세 분석
    const inventoryTrendResult = await db.execute(sql`
      SELECT 
        'LT3M' as period,
        SUM(CAST("LT3M(수량)" AS DECIMAL)) as quantity,
        SUM(CAST("LT3M(금액)" AS DECIMAL)) as amount
      FROM sap_zmmr0016_inventory
      UNION ALL
      SELECT 
        'LT6M' as period,
        SUM(CAST("LT6M(수량)" AS DECIMAL)) as quantity,
        SUM(CAST("LT6M(금액)" AS DECIMAL)) as amount
      FROM sap_zmmr0016_inventory
      UNION ALL
      SELECT 
        'LT12M' as period,
        SUM(CAST("LT12M(수량)" AS DECIMAL)) as quantity,
        SUM(CAST("LT12M(금액)" AS DECIMAL)) as amount
      FROM sap_zmmr0016_inventory
      UNION ALL
      SELECT 
        'LT24M' as period,
        SUM(CAST("LT24M(수량)" AS DECIMAL)) as quantity,
        SUM(CAST("LT24M(금액)" AS DECIMAL)) as amount
      FROM sap_zmmr0016_inventory
      ORDER BY period
    `);

    return NextResponse.json({
      kpi: {
        totalInventoryValue:
          inventoryTotalResult[0]?.total_inventory_value || 0,
        totalInventoryQuantity:
          inventoryTotalResult[0]?.total_inventory_quantity || 0,
        currentMonthSales: currentMonthSalesResult[0]?.current_month_sales || 0,
        activeMaterials: activeMaterialsResult[0]?.active_materials || 0,
        avgTurnoverRate:
          Number(inventoryTurnoverResult[0]?.avg_turnover_rate) || 0,
        slowMovingCount:
          Number(inventoryTurnoverResult[0]?.slow_moving_count) || 0,
        obsoleteCount: Number(inventoryTurnoverResult[0]?.obsolete_count) || 0,
        fulfillmentRate:
          Number(orderFulfillmentResult[0]?.fulfillment_rate) || 0,
      },
      shortageTop5: shortageResult,
      monthlySales: monthlySalesResult,
      salesByProductGroup: salesByProductGroupResult,
      salesByProductType: salesByProductTypeResult,
      inventoryByStatus: inventoryByStatusResult,
      abcInventory: abcInventoryResult[0],
      topCustomers: topCustomersResult,
      inventoryTrend: inventoryTrendResult,
      orderFulfillment: orderFulfillmentResult[0],
    });
  } catch (error) {
    console.error('Dashboard summary error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch dashboard data' },
      { status: 500 },
    );
  }
}
