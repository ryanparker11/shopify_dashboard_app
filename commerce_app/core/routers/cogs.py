from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from commerce_app.core.db import get_conn
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
import io

router = APIRouter()

@router.get("/cogs/download-template")
async def download_cogs_template(shop_domain: str):
    """
    Generate and download COGS upload template with SKU, NAME, and VARIANT pre-filled.
    User only needs to fill in the COGS column.
    """
    try:
        # Fetch products and variants from database
        sql = """
        SELECT 
            pv.sku,
            p.title as product_name,
            pv.title as variant_title
        FROM shopify.product_variants pv
        JOIN shopify.products p ON pv.product_id = p.product_id
        WHERE p.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
        ORDER BY p.title, pv.title;
        """
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (shop_domain,))
                products_data = await cur.fetchall()
                
                if not products_data:
                    raise HTTPException(404, "No products found for this shop")
        
        # Generate Excel template
        wb = Workbook()
        sheet = wb.active
        sheet.title = "COGS Upload"
        
        # Header row styling
        headers = ['SKU', 'NAME', 'VARIANT', 'COGS']
        for col, header in enumerate(headers, start=1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', start_color='4472C4')
            cell.alignment = Alignment(horizontal='center', vertical='center')
        
        # Set column widths
        sheet.column_dimensions['A'].width = 20  # SKU
        sheet.column_dimensions['B'].width = 40  # NAME
        sheet.column_dimensions['C'].width = 30  # VARIANT
        sheet.column_dimensions['D'].width = 15  # COGS
        
        # Populate data rows
        for row_idx, (sku, product_name, variant_title) in enumerate(products_data, start=2):
            sheet.cell(row=row_idx, column=1, value=sku or '')
            sheet.cell(row=row_idx, column=2, value=product_name or '')
            sheet.cell(row=row_idx, column=3, value=variant_title if variant_title != 'Default Title' else 'Default')
            
            # COGS column - yellow highlight for user input
            cogs_cell = sheet.cell(row=row_idx, column=4)
            cogs_cell.fill = PatternFill('solid', start_color='FFFF00')
        
        # Freeze header row
        sheet.freeze_panes = 'A2'
        
        # Save to BytesIO
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        
        # Return as downloadable file
        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': 'attachment; filename=cogs_upload_template.xlsx'
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")


@router.get("/cogs/summary")
async def cogs_summary(shop_domain: str):
    """
    Get COGS summary statistics for the shop
    """
    sql = """
    SELECT
        COUNT(DISTINCT pv.variant_id)::int AS total_variants,
        COUNT(DISTINCT CASE WHEN pv.cost IS NOT NULL THEN pv.variant_id END)::int AS variants_with_cogs,
        COALESCE(AVG(pv.cost), 0)::numeric AS avg_cogs
    FROM shopify.product_variants pv
    JOIN shopify.products p ON pv.product_id = p.product_id
    WHERE p.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain,))
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(404, "Shop not found")
            
            total_variants, variants_with_cogs, avg_cogs = row
            
            return {
                "total_variants": total_variants,
                "variants_with_cogs": variants_with_cogs,
                "variants_without_cogs": total_variants - variants_with_cogs,
                "avg_cogs": float(avg_cogs) if avg_cogs else 0.0,
                "cogs_coverage_percentage": round((variants_with_cogs / total_variants * 100), 2) if total_variants > 0 else 0.0
            }