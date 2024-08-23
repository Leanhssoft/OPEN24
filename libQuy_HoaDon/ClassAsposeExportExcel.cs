using Aspose.Cells;
using Microsoft.Office.Interop.Excel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace libQuy_HoaDon
{
    public class ClassExcel_CellData
    {
        public int RowIndex { get; set; }
        public int ColumnIndex { get; set; }
        public string CellValue { get; set; }// tiêu đề Data của file xuất ra (mặc định ghi ở dòng Cell[1,1]
        public bool IsNumber { get; set; }
    }

    public class Excel_ParamExport
    {
        public bool? HasRowSum_AtLastIndex { get; set; } = false;
        public int SheetIndex { get; set; }
        public List<ClassExcel_CellData> CellData { get; set; }
        public int StartRow { get; set; }// vị trí bắt đầu của dòng dữ liệu (trong file mẫu)
        public int? EndRow { get; set; } = 30;// vị trí kết thúc của dòng dữ liệu (trong file mẫu)
        public string ColumnsHide { get; set; } // mảng các cột bị xóa: ngăn cách = dấu gạch dưới (4_8_3_)
    }
    public class ClassAsposeExportExcel
    {
        private void RemoveColumn(Aspose.Cells.Worksheet wSheet, string columnsHide)
        {
            if (!string.IsNullOrEmpty(columnsHide))
            {
                string[] coloumHide = columnsHide.Split('_');
                coloumHide = coloumHide.Where(x => x != "").Distinct().ToArray();
                var columH = Array.ConvertAll(coloumHide, int.Parse).OrderByDescending(x => x).ToArray();
                for (int i = 0; i < columH.Length; i++)
                {
                    wSheet.Cells.DeleteColumn(columH[i]);
                }
            }
        }
        public List<ClassExcel_CellData> GetData_ForDefaultCell(string tenChiNhanh, string timeReport)
        {
            timeReport = timeReport.Contains("Thời gian") ? timeReport : string.Concat("Thời gian: ", timeReport);
            List<ClassExcel_CellData> lstCell = new List<ClassExcel_CellData>
            {
                new ClassExcel_CellData { RowIndex = 1, ColumnIndex = 0, CellValue =  timeReport },
                new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 0, CellValue = "Chi nhánh: " + tenChiNhanh }
            };
            return lstCell;
        }

        public List<ClassExcel_CellData> GetData_ForDefaultCellv2(string tenChiNhanh, string timeReport)
        {
            timeReport = timeReport.Contains("Thời gian") ? timeReport.Replace("Thời gian", "").Trim() : timeReport;
            List<ClassExcel_CellData> lstCell = new List<ClassExcel_CellData>
            {
                new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 0, CellValue =  "Thời gian: " },
                new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 1, CellValue =  timeReport },
                new ClassExcel_CellData { RowIndex = 3, ColumnIndex = 0, CellValue = "Chi nhánh: " },
                new ClassExcel_CellData { RowIndex = 3, ColumnIndex = 1, CellValue = tenChiNhanh }
            };
            return lstCell;
        }
        private void SetData_ToDefaultCell(Aspose.Cells.Worksheet wSheet, List<ClassExcel_CellData> lst)
        {
            if (lst != null)
            {
                foreach (var item in lst)
                {
                    wSheet.Cells[item.RowIndex, item.ColumnIndex].Value = item.CellValue;
                }
            }
        }
        private HttpResponseMessage ReturnFileExcel_ToBrower(Aspose.Cells.Workbook workbook)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                workbook.Save(memoryStream, SaveFormat.Xlsx); // Lưu dưới định dạng .xlsx
                memoryStream.Position = 0; // Đặt lại vị trí của stream về đầu

                // Tạo HttpResponseMessage
                var response = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(memoryStream.ToArray())
                };
                response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                response.Content.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("attachment");
                return response;
            }
        }

        public HttpResponseMessage ExportData_ToMultipleSheet(string templatePath, List<System.Data.DataTable> lstDataTable, List<Excel_ParamExport> lstPr = null)
        {
            // Tạo đối tượng Workbook và mở file Excel từ đường dẫn templatePath
            Aspose.Cells.Workbook workbook;
            using (var fileStream = new FileStream(templatePath, FileMode.Open, FileAccess.Read))
            {
                workbook = new Aspose.Cells.Workbook(fileStream);
            }
            foreach (var pr in lstPr)
            {
                ExportDetailData_ToExcel(workbook, pr.SheetIndex, lstDataTable[pr.SheetIndex], pr.StartRow, pr.EndRow ?? 30, pr.HasRowSum_AtLastIndex ?? false, pr.ColumnsHide, pr.CellData);
            }
            var response = ReturnFileExcel_ToBrower(workbook);
            return response;
        }
        public HttpResponseMessage ExportData_ToOneSheet(string tempPath, System.Data.DataTable tblDuLieu,
           int sourceRowIndex, int destinationRowIndex, bool hasRowSum, string columnsHide,
            List<ClassExcel_CellData> lst = null)
        {
            List<System.Data.DataTable> lstDataTable = new List<System.Data.DataTable>
            {
                tblDuLieu
            };
            List<Excel_ParamExport> lstPr = new List<Excel_ParamExport> {
                 new Excel_ParamExport
                 {
                    SheetIndex=0,
                    StartRow =  sourceRowIndex,
                    EndRow = destinationRowIndex,
                    ColumnsHide = columnsHide ,
                    HasRowSum_AtLastIndex = hasRowSum,
                    CellData = lst
                 }
            };
            return ExportData_ToMultipleSheet(tempPath, lstDataTable, lstPr);
        }
        public void ExportDetailData_ToExcel(Aspose.Cells.Workbook wbook, int sheetIndex, System.Data.DataTable tblDuLieu,
            int sourceRowIndex, int destinationRowIndex, bool hasRowSum, string columnsHide,
            List<ClassExcel_CellData> lst = null)
        {
            Aspose.Cells.Worksheet wSheet = wbook.Worksheets[sheetIndex];

            int rowNumber = destinationRowIndex - sourceRowIndex;
            int dkrange = (tblDuLieu.Rows.Count) / rowNumber;
            if (dkrange >= 1)
            {
                //Chèn dòng tổng cộng
                if (hasRowSum)
                {
                    wSheet.Cells.CopyRows(wSheet.Cells, destinationRowIndex, tblDuLieu.Rows.Count + sourceRowIndex, 10);
                }
            }
            if (dkrange < 1)
            {
                wSheet.Cells.DeleteRows(sourceRowIndex + tblDuLieu.Rows.Count, rowNumber - tblDuLieu.Rows.Count);
            }
            for (int i = 1; i < dkrange; i++)
            {
                wSheet.Cells.CopyRows(wSheet.Cells, sourceRowIndex, (rowNumber * i) + sourceRowIndex, rowNumber);
            }
            if (dkrange * rowNumber < tblDuLieu.Rows.Count)
            {
                wSheet.Cells.CopyRows(wSheet.Cells, sourceRowIndex, (dkrange * rowNumber) + sourceRowIndex, tblDuLieu.Rows.Count - dkrange * rowNumber);
            }
            wSheet.Cells.ImportDataTable(tblDuLieu, false, sourceRowIndex, 0, false);

            RemoveColumn(wSheet, columnsHide);
            SetData_ToDefaultCell(wSheet, lst);
        }
    }
}
