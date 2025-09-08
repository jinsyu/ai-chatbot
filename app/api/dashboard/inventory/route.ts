import { NextResponse } from 'next/server';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';

// Create database connection
const client = postgres(process.env.POSTGRES_URL!);
const db = drizzle(client);

export async function GET() {
  try {
    // 1. 재고 회전율 상위/하위 제품
    const inventoryTurnoverResult = await db.execute(sql`
      WITH turnover_calc AS (
        SELECT 
          i."자재",
          i."자재명",
          i."제품군명",
          CAST(i."재고금액" AS DECIMAL) as inventory_value,
          CAST(i."6개월평균판매금액" AS DECIMAL) as avg_sales,
          CASE 
            WHEN CAST(i."재고금액" AS DECIMAL) > 0 
            THEN CAST(i."6개월평균판매금액" AS DECIMAL) * 2 / CAST(i."재고금액" AS DECIMAL)
            ELSE 0 
          END as turnover_rate
        FROM sap_zmmr0016_inventory i
        WHERE CAST(i."재고금액" AS DECIMAL) > 0
      )
      SELECT * FROM (
        SELECT *, 'top' as category FROM turnover_calc ORDER BY turnover_rate DESC LIMIT 10
        UNION ALL
        SELECT *, 'bottom' as category FROM turnover_calc WHERE turnover_rate > 0 ORDER BY turnover_rate ASC LIMIT 10
      ) combined
      ORDER BY category, turnover_rate DESC
    `);

    // 2. ABC 재고 상세 분석
    const abcDetailResult = await db.execute(sql`
      SELECT 
        i."재고구분명" as inventory_type,
        COUNT(DISTINCT i."자재") as material_count,
        SUM(CAST(i."가용재고금액" AS DECIMAL)) as available_value,
        SUM(CAST(i."B등급" AS DECIMAL)) as b_grade,
        SUM(CAST(i."C등급" AS DECIMAL)) as c_grade,
        SUM(CAST(i."D등급" AS DECIMAL)) as d_grade,
        SUM(CAST(i."등급재고금액" AS DECIMAL)) as grade_total_value
      FROM sap_zmmr0016_inventory i
      GROUP BY i."재고구분명"
      ORDER BY available_value DESC
    `);

    // 3. 재고 보유 기간별 분석
    const agingAnalysisResult = await db.execute(sql`
      SELECT 
        CASE 
          WHEN CAST("LT3M(수량)" AS DECIMAL) > 0 THEN '3개월 이내'
          WHEN CAST("LT6M(수량)" AS DECIMAL) > 0 THEN '3-6개월'
          WHEN CAST("LT12M(수량)" AS DECIMAL) > 0 THEN '6-12개월'
          WHEN CAST("LT24M(수량)" AS DECIMAL) > 0 THEN '12-24개월'
          ELSE '24개월 초과'
        END as aging_period,
        COUNT(*) as item_count,
        SUM(CAST("재고금액" AS DECIMAL)) as total_value,
        SUM(CAST("가용재고수량" AS DECIMAL)) as total_quantity
      FROM sap_zmmr0016_inventory
      GROUP BY 
        CASE 
          WHEN CAST("LT3M(수량)" AS DECIMAL) > 0 THEN '3개월 이내'
          WHEN CAST("LT6M(수량)" AS DECIMAL) > 0 THEN '3-6개월'
          WHEN CAST("LT12M(수량)" AS DECIMAL) > 0 THEN '6-12개월'
          WHEN CAST("LT24M(수량)" AS DECIMAL) > 0 THEN '12-24개월'
          ELSE '24개월 초과'
        END
      ORDER BY 
        CASE 
          WHEN CAST("LT3M(수량)" AS DECIMAL) > 0 THEN 1
          WHEN CAST("LT6M(수량)" AS DECIMAL) > 0 THEN 2
          WHEN CAST("LT12M(수량)" AS DECIMAL) > 0 THEN 3
          WHEN CAST("LT24M(수량)" AS DECIMAL) > 0 THEN 4
          ELSE 5
        END
    `);

    // 4. 불용재고 상세
    const obsoleteInventoryResult = await db.execute(sql`
      SELECT 
        i."자재",
        i."자재명",
        i."제품군명",
        m."자재그룹7명" as product_group,
        CAST(i."재고금액" AS DECIMAL) as inventory_value,
        CAST(i."가용재고수량" AS DECIMAL) as available_quantity,
        CAST(i."MT24M(수량)" AS DECIMAL) as obsolete_quantity,
        CAST(i."MT24M(금액)" AS DECIMAL) as obsolete_value
      FROM sap_zmmr0016_inventory i
      LEFT JOIN sap_zmmr0001_materials m ON i."자재" = m."자재"
      WHERE CAST(i."MT24M(수량)" AS DECIMAL) > 0
      ORDER BY CAST(i."MT24M(금액)" AS DECIMAL) DESC
      LIMIT 20
    `);

    // 5. 위탁재고 및 CY재고 현황
    const specialInventoryResult = await db.execute(sql`
      SELECT 
        SUM(CAST("위탁재고" AS DECIMAL)) as consignment_stock,
        SUM(CAST("CY재고" AS DECIMAL)) as cy_stock,
        SUM(CAST("보류재고" AS DECIMAL)) as blocked_stock,
        SUM(CAST("생산재작업" AS DECIMAL)) as rework_stock,
        COUNT(CASE WHEN CAST("위탁재고" AS DECIMAL) > 0 THEN 1 END) as consignment_items,
        COUNT(CASE WHEN CAST("CY재고" AS DECIMAL) > 0 THEN 1 END) as cy_items
      FROM sap_zmmr0016_inventory
    `);

    // 6. 제품군별 재고 효율성
    const efficiencyByGroupResult = await db.execute(sql`
      SELECT 
        m."자재그룹7명" as product_group,
        COUNT(DISTINCT i."자재") as material_count,
        SUM(CAST(i."재고금액" AS DECIMAL)) as total_value,
        SUM(CAST(i."가용재고수량" AS DECIMAL)) as total_quantity,
        AVG(CASE 
          WHEN CAST(i."재고금액" AS DECIMAL) > 0 
          THEN CAST(i."6개월평균판매금액" AS DECIMAL) * 2 / CAST(i."재고금액" AS DECIMAL)
          ELSE 0 
        END) as avg_turnover_rate,
        SUM(CAST(i."6개월평균판매금액" AS DECIMAL)) as avg_sales_value
      FROM sap_zmmr0016_inventory i
      LEFT JOIN sap_zmmr0001_materials m ON i."자재" = m."자재"
      WHERE m."자재그룹7명" IS NOT NULL
      GROUP BY m."자재그룹7명"
      ORDER BY total_value DESC
    `);

    // 7. 공급업체별 재고
    const supplierInventoryResult = await db.execute(sql`
      SELECT 
        "공급업체" as supplier,
        COUNT(DISTINCT "자재") as material_count,
        SUM(CAST("재고금액" AS DECIMAL)) as total_value,
        SUM(CAST("가용재고수량" AS DECIMAL)) as total_quantity,
        AVG(CAST("6개월평균판매수량" AS DECIMAL)) as avg_sales_quantity
      FROM sap_zmmr0016_inventory
      WHERE "공급업체" IS NOT NULL
      GROUP BY "공급업체"
      ORDER BY total_value DESC
      LIMIT 20
    `);

    // 8. 재고 위험도 분석
    const riskAnalysisResult = await db.execute(sql`
      WITH risk_calc AS (
        SELECT 
          i."자재",
          i."자재명",
          i."제품군명",
          CAST(i."재고금액" AS DECIMAL) as inventory_value,
          CAST(i."가용재고수량" AS DECIMAL) as available_qty,
          CAST(i."6개월평균판매수량" AS DECIMAL) as avg_sales_qty,
          CASE 
            WHEN CAST(i."6개월평균판매수량" AS DECIMAL) > 0 
            THEN CAST(i."가용재고수량" AS DECIMAL) / CAST(i."6개월평균판매수량" AS DECIMAL)
            ELSE 999 
          END as months_of_stock,
          CASE
            WHEN CAST(i."MT24M(수량)" AS DECIMAL) > 0 THEN 'Critical'
            WHEN CAST(i."LT24M(수량)" AS DECIMAL) = 0 AND CAST(i."LT12M(수량)" AS DECIMAL) = 0 THEN 'High'
            WHEN CAST(i."LT12M(수량)" AS DECIMAL) = 0 AND CAST(i."LT6M(수량)" AS DECIMAL) = 0 THEN 'Medium'
            ELSE 'Low'
          END as risk_level
        FROM sap_zmmr0016_inventory i
        WHERE CAST(i."재고금액" AS DECIMAL) > 0
      )
      SELECT 
        risk_level,
        COUNT(*) as item_count,
        SUM(inventory_value) as total_value
      FROM risk_calc
      GROUP BY risk_level
      ORDER BY 
        CASE risk_level
          WHEN 'Critical' THEN 1
          WHEN 'High' THEN 2
          WHEN 'Medium' THEN 3
          WHEN 'Low' THEN 4
        END
    `);

    return NextResponse.json({
      inventoryTurnover: inventoryTurnoverResult,
      abcDetail: abcDetailResult,
      agingAnalysis: agingAnalysisResult,
      obsoleteInventory: obsoleteInventoryResult,
      specialInventory: specialInventoryResult[0],
      efficiencyByGroup: efficiencyByGroupResult,
      supplierInventory: supplierInventoryResult,
      riskAnalysis: riskAnalysisResult,
    });
  } catch (error) {
    console.error('Inventory analysis error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch inventory data' },
      { status: 500 },
    );
  }
}