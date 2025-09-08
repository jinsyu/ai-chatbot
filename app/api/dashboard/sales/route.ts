import { NextResponse } from 'next/server';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';

// Create database connection
const client = postgres(process.env.POSTGRES_URL!);
const db = drizzle(client);

export async function GET() {
  try {
    // 1. 일별 매출 추이 (최근 30일)
    const dailySalesResult = await db.execute(sql`
      SELECT 
        TO_DATE("청구일", 'YYYY-MM-DD') as date,
        SUM(CAST("청구금액" AS DECIMAL)) as sales,
        COUNT(DISTINCT "대금청구문서") as invoice_count,
        COUNT(DISTINCT "판매처") as customer_count
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '30 days'
      GROUP BY TO_DATE("청구일", 'YYYY-MM-DD')
      ORDER BY date
    `);

    // 2. 제품군별 매출 분석
    const salesByProductGroupResult = await db.execute(sql`
      SELECT 
        COALESCE("자재그룹7명", '기타') as product_group,
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        SUM(CAST("청구수량" AS DECIMAL)) as total_quantity,
        COUNT(DISTINCT "자재") as material_count,
        AVG(CAST("판가" AS DECIMAL)) as avg_price,
        AVG(CAST("매출할인" AS DECIMAL)) as avg_discount
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY COALESCE("자재그룹7명", '기타')
      ORDER BY total_sales DESC
    `);

    // 3. 판매처별 매출 추이
    const customerSalesTrendResult = await db.execute(sql`
      SELECT 
        "판매처명",
        TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-MM') as month,
        SUM(CAST("청구금액" AS DECIMAL)) as sales
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months')
        AND "판매처명" IN (
          SELECT "판매처명"
          FROM sap_zsdr0340_sales_detail
          GROUP BY "판매처명"
          ORDER BY SUM(CAST("청구금액" AS DECIMAL)) DESC
          LIMIT 5
        )
      GROUP BY "판매처명", TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-MM')
      ORDER BY month, sales DESC
    `);

    // 4. 설계처별 매출 분석
    const salesByDesignerResult = await db.execute(sql`
      SELECT 
        "설계처명",
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        COUNT(DISTINCT "대금청구문서") as invoice_count,
        COUNT(DISTINCT "자재") as material_variety,
        AVG(CAST("본사마진" AS DECIMAL)) as avg_margin
      FROM sap_zsdr0340_sales_detail
      WHERE "설계처명" IS NOT NULL
        AND TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY "설계처명"
      ORDER BY total_sales DESC
      LIMIT 20
    `);

    // 5. 오더 유형별 분석
    const orderTypeAnalysisResult = await db.execute(sql`
      SELECT 
        "오더TYPE" as order_type,
        COUNT(*) as order_count,
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        AVG(CAST("청구금액" AS DECIMAL)) as avg_sales
      FROM sap_zsdr0340_sales_detail
      WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY "오더TYPE"
      ORDER BY total_sales DESC
    `);

    // 6. 분기별 매출 성장률
    const quarterlyGrowthResult = await db.execute(sql`
      WITH quarterly_sales AS (
        SELECT 
          TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-Q') as quarter,
          SUM(CAST("청구금액" AS DECIMAL)) as sales
        FROM sap_zsdr0340_sales_detail
        WHERE TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')
        GROUP BY TO_CHAR(TO_DATE("청구일", 'YYYY-MM-DD'), 'YYYY-Q')
      )
      SELECT 
        quarter,
        sales,
        LAG(sales, 1) OVER (ORDER BY quarter) as prev_sales,
        CASE 
          WHEN LAG(sales, 1) OVER (ORDER BY quarter) > 0 
          THEN ((sales - LAG(sales, 1) OVER (ORDER BY quarter)) / LAG(sales, 1) OVER (ORDER BY quarter) * 100)
          ELSE 0 
        END as growth_rate
      FROM quarterly_sales
      ORDER BY quarter
    `);

    // 7. 베스트셀러 제품
    const bestSellingProductsResult = await db.execute(sql`
      SELECT 
        s."자재",
        s."자재명",
        m."자재그룹7명" as product_group,
        SUM(CAST(s."청구수량" AS DECIMAL)) as total_quantity,
        SUM(CAST(s."청구금액" AS DECIMAL)) as total_sales,
        COUNT(DISTINCT s."판매처") as customer_count,
        AVG(CAST(s."판가" AS DECIMAL)) as avg_price
      FROM sap_zsdr0340_sales_detail s
      LEFT JOIN sap_zmmr0001_materials m ON s."자재" = m."자재"
      WHERE TO_DATE(s."청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY s."자재", s."자재명", m."자재그룹7명"
      ORDER BY total_sales DESC
      LIMIT 20
    `);

    // 8. 지역별 매출
    const salesByRegionResult = await db.execute(sql`
      SELECT 
        "판매처지역명" as region,
        COUNT(DISTINCT "판매처") as customer_count,
        SUM(CAST("청구금액" AS DECIMAL)) as total_sales,
        AVG(CAST("청구금액" AS DECIMAL)) as avg_sales
      FROM sap_zsdr0340_sales_detail
      WHERE "판매처지역명" IS NOT NULL
        AND TO_DATE("청구일", 'YYYY-MM-DD') >= DATE_TRUNC('month', CURRENT_DATE)
      GROUP BY "판매처지역명"
      ORDER BY total_sales DESC
    `);

    return NextResponse.json({
      dailySales: dailySalesResult,
      salesByProductGroup: salesByProductGroupResult,
      customerSalesTrend: customerSalesTrendResult,
      salesByDesigner: salesByDesignerResult,
      orderTypeAnalysis: orderTypeAnalysisResult,
      quarterlyGrowth: quarterlyGrowthResult,
      bestSellingProducts: bestSellingProductsResult,
      salesByRegion: salesByRegionResult,
    });
  } catch (error) {
    console.error('Sales analysis error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch sales data' },
      { status: 500 },
    );
  }
}