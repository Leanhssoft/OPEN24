﻿using banhang24.Hellper;
using banhang24.Models;
using libDM_DoiTuong;
using libDM_HangHoa;
using libHT;
using libHT_NguoiDung;
using libQuy_HoaDon;
using libReport;
using Model;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;
using System.Web.Http;
using System.Web.Http.Results;
using static banhang24.Hellper.commonEnum;


namespace banhang24.Areas.DanhMuc.Controllers
{
    public class ReportAPIController : BaseApiController
    {
        #region public
        public List<ListLHPages> getAllPage<T>(List<T> lstLHs, float sohang)
        {
            List<ListLHPages> listPage = new List<ListLHPages>();
            if (lstLHs != null)
            {
                int dem = 1;
                float SoTrang = lstLHs.Count / sohang;
                for (int i = 0; i < SoTrang; i++)
                {
                    ListLHPages LH_page = new ListLHPages();
                    LH_page.SoTrang = dem;
                    listPage.Add(LH_page);
                    dem = dem + 1;
                }
                return listPage;
            }
            else
            {
                return null;
            }
        }
        public List<ListLHPages> getlist_Page(float sohang, int pageSize)
        {
            List<ListLHPages> listPage = new List<ListLHPages>();
            if (sohang > 0)
            {
                int dem = 1;
                float SoTrang = sohang / pageSize;
                for (int i = 0; i < SoTrang; i++)
                {
                    ListLHPages LH_page = new ListLHPages();
                    LH_page.SoTrang = dem;
                    listPage.Add(LH_page);
                    dem = dem + 1;
                }
                return listPage;
            }
            else
            {
                return listPage;
            }
        }
        public int getNumber_Page(float sohang, int pageSize)
        {
            if (sohang > 0)
            {
                float SoTrang = sohang / pageSize;
                if (SoTrang > (int)SoTrang)
                    return (int)SoTrang + 1;
                else
                    return (int)SoTrang;
            }
            else
            {
                return 0;
            }
        }
        public int getRowsCountList<T>(List<T> lstLHs)
        {
            if (lstLHs != null)
            {
                return lstLHs.Count;
            }
            else
            {
                return 0;
            }
        }
        #endregion
        #region  Báo cáo hàng hóa theo bán hàng
        [HttpGet]
        public List<Report_HangHoa_BanHang> TongGiaTriHH_BanHang(string maHH, DateTime timeStart, DateTime timeEnd, int laHangHoa, int pageNumber, int pageSize, Guid? ID_NhomHang, Guid ID_ChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<Report_HangHoa_BanHang> lst = report.TongGiaTriHH_BanHang(maHH, timeStart, timeEnd, laHangHoa, ID_NhomHang, ID_ChiNhanh);
                return lst;
            }
        }
        #endregion
        #region Báo cáo hàng hóa tồn kho
        [HttpGet]
        public List<Report_DM_LoaiChungTuPRC> getListDM_LoaiChungTu(string LoaiChungTu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                libDM_LoaiChungTu.ClassDM_LoaiChungTu loaiChungTu = new libDM_LoaiChungTu.ClassDM_LoaiChungTu(db);
                List<int> lstLoaiChungTu = LoaiChungTu.Split(',').Select(Int32.Parse).ToList();
                List<Report_DM_LoaiChungTuPRC> lstResultLoaiChungTu = loaiChungTu.Gets(p => lstLoaiChungTu.Contains(p.ID)).Select(p => new Report_DM_LoaiChungTuPRC
                {
                    ID = p.ID,
                    TenChungTu = p.TenLoaiChungTu
                }).ToList();
                return lstResultLoaiChungTu;
            }
        }
        #endregion

        #region báo cáo tài chính
        public List<ListYear> getListYear()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportFilter reportFilter = new ClassReportFilter(db);
                List<ListYear> lst = reportFilter.getListYear();
                return lst;
            }

        }
        public List<Report_TaiChinh_TheoThang> getListTaiChinh_TheoThang(int year, string ID_ChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<Report_TaiChinh_TheoThang> lst = reportTaiChinh.getListTaiChinh_TheoThang(year, ID_ChiNhanh);
                return lst;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoKetQuaHoatDongKinhDoanhTheoThang([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<Report_TaiChinh_TheoThang> lst = new List<Report_TaiChinh_TheoThang>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoThang_Gara(Year, IdChiNhanh);
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoThang(Year, IdChiNhanh);
                    }
                }
                return ActionTrueData(new
                {
                    data = lst
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }
        public List<Report_TaiChinh_TheoQuy> getListTaiChinh_TheoQuy(int year, string ID_ChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<Report_TaiChinh_TheoQuy> lst = reportTaiChinh.getListTaiChinh_TheoQuy(year, ID_ChiNhanh);
                return lst;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoKetQuaHoatDongKinhDoanhTheoQuy([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<Report_TaiChinh_TheoQuy> lst = new List<Report_TaiChinh_TheoQuy>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoQuy_Gara(Year, IdChiNhanh);
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoQuy(Year, IdChiNhanh);
                    }
                }
                return ActionTrueData(new
                {
                    data = lst
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }
        public List<Report_TaiChinh_TheoNam> getListTaiChinh_TheoNam(int year, string ID_ChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<Report_TaiChinh_TheoNam> lst = reportTaiChinh.getListTaiChinh_TheoNam(year, ID_ChiNhanh);
                return lst;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoKetQuaHoatDongKinhDoanhTheoNam([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<Report_TaiChinh_TheoNam> lst = new List<Report_TaiChinh_TheoNam>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    lst = reportTaiChinh.getListTaiChinh_TheoNam(Year, IdChiNhanh);
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoNam_Gara(Year, IdChiNhanh);
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoNam(Year, IdChiNhanh);
                    }
                }
                return ActionTrueData(new
                {
                    data = lst
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        #endregion
        #region báo cáo cuối ngày

        #endregion
        #region lịch sử thao tác
        [HttpPost]
        public System.Web.Http.Results.JsonResult<JsonResultExample<LichSuThaoTac>> getList_LichSuThaoTac(array_LichSuThaoTac array_Search)
        {
            List<LichSuThaoTac> lst = new List<LichSuThaoTac>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                classNhatKySuDung nhatKySuDung = new classNhatKySuDung(db);
                string NoiDung_Search = "%%";
                if (array_Search.NoiDung != "" & array_Search.NoiDung != null & array_Search.NoiDung != "null")
                    NoiDung_Search = "%" + array_Search.NoiDung + "%";
                string ChucNang_Search = "%%";
                if (array_Search.ChucNang != "" & array_Search.ChucNang != null & array_Search.ChucNang != "null")
                    ChucNang_Search = "%" + array_Search.ChucNang + "%";
                lst = nhatKySuDung.GetListLichSuThaoTac(array_Search.ID_NhanVien, array_Search.ID_ChiNhanh, NoiDung_Search, ChucNang_Search, array_Search.timeStart, array_Search.timeEnd, array_Search.ThaoTac,
                    array_Search.XemDS_PhongBan, array_Search.XemDS_HeThong, array_Search.ID_NguoiDung);
                int Rows = lst.Count;
                List<LichSuThaoTac> lstNK = lst.Skip((array_Search.pageNumber - 1) * array_Search.pageSize).Take(array_Search.pageSize).ToList();
                List<ListLHPages> lstPage = getAllPage<LichSuThaoTac>(lst, array_Search.pageSize);
                JsonResultExample<LichSuThaoTac> jsonobj = new JsonResultExample<LichSuThaoTac>
                {
                    LstData = lstNK,
                    Rowcount = Rows,
                    LstPageNumber = lstPage
                };
                return Json(jsonobj);
            }
        }
        // xuất excel nhật ký
        [HttpPost]
        public string ExportExcel_LichSuThaoTac([FromBody] JObject data)
        {
            array_LichSuThaoTac array_Search = data["objExcel"].ToObject<array_LichSuThaoTac>();
            List<LichSuThaoTac> lst = new List<LichSuThaoTac>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                classNhatKySuDung nhatKySuDung = new classNhatKySuDung(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                string NoiDung_Search = "%%";
                if (array_Search.NoiDung != "" & array_Search.NoiDung != null & array_Search.NoiDung != "null")
                    NoiDung_Search = "%" + array_Search.NoiDung + "%";
                string ChucNang_Search = "%%";
                if (array_Search.ChucNang != "" & array_Search.ChucNang != null & array_Search.ChucNang != "null")
                    ChucNang_Search = "%" + array_Search.ChucNang + "%";
                lst = nhatKySuDung.GetListLichSuThaoTac(array_Search.ID_NhanVien, array_Search.ID_ChiNhanh, NoiDung_Search, ChucNang_Search, array_Search.timeStart, array_Search.timeEnd, array_Search.ThaoTac,
                    array_Search.XemDS_PhongBan, array_Search.XemDS_HeThong, array_Search.ID_NguoiDung);
                DataTable excel_TH = classOffice.ToDataTable<LichSuThaoTac>(lst);
                excel_TH.Columns.Remove("NoiDungChiTiet");
                excel_TH.Columns.Remove("ChucNang_CV");
                excel_TH.Columns.Remove("ChucNang_GC");
                excel_TH.Columns.Remove("NoiDung_CV");
                excel_TH.Columns.Remove("NoiDung_GC");
                string fileTeamplateTH = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCao/Teamplate_LichSuThaoTac.xlsx");
                string fileSaveTH = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCao/LichSuThaoTac.xlsx");
                classOffice.listToOfficeExcel_Stype(fileTeamplateTH, fileSaveTH, excel_TH, 4, 28, 24, false, array_Search.columnsHide, array_Search.txtTime, array_Search.nameChiNhanh);
                HttpResponse Response2 = HttpContext.Current.Response;
                //string file_return = "~/Template/ExportExcel/BaoCao/LichSuThaoTac.xlsx";
                string file_return = "~/Template/ExportExcel/BaoCao/LichSuThaoTac.xlsx";
                return file_return;
                //classOffice.downloadFile(fileSaveTH);
            }
        }
        #endregion
        public List<Report_NhomHangHoa_byName> GetListID_NhomHangHoa(string TenNhomHang)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportFilter reportFilter = new ClassReportFilter(db);
                List<Report_NhomHangHoa_byName> lst = new List<Report_NhomHangHoa_byName>();
                lst = reportFilter.getList_ID_NhomHangHoa_ByName(lst, TenNhomHang);
                return lst;
            }
        }

        public IEnumerable<Object> GetListID_NhomHangHoaByTen(string TenNhomHang)
        {
            var lst = Class_Report.getlistNhomHHByTenNhom(TenNhomHang);
            return lst;
        }

        public List<Report_NhomDoiTuong_ByName> GetListID_NhomDoiTuong(string TenNhomDoiTuong, int loaidoituong)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                libDM_NhomDoiTuong.ClassDM_NhomDoiTuong nhomDoiTuong = new libDM_NhomDoiTuong.ClassDM_NhomDoiTuong(db);

                List<Report_NhomDoiTuong_ByName> lst = new List<Report_NhomDoiTuong_ByName>();
                lst = nhomDoiTuong.Gets(null).Select(p => new Report_NhomDoiTuong_ByName
                {
                    ID = p.ID,
                    TenNhomDoiTuong = p.TenNhomDoiTuong,
                    TenNhomDoiTuong_KhongDau = p.TenNhomDoiTuong_KhongDau,
                    TenNhomDoiTuong_KyTuDau = p.TenNhomDoiTuong_KyTuDau
                }).OrderBy(p => p.TenNhomDoiTuong).ToList();
                lst.Insert(0, new Report_NhomDoiTuong_ByName(new Guid("00000000-0000-0000-0000-000000000000"), "Nhóm mặc định", "Nhom mac dinh", "Nmd"));
                if (TenNhomDoiTuong != "" & TenNhomDoiTuong != "null" & TenNhomDoiTuong != null)
                {
                    TenNhomDoiTuong = CommonStatic.ConvertToUnSign(TenNhomDoiTuong).ToLower();
                    lst = lst.Where(x => x.TenNhomDoiTuong_KhongDau.Contains(@TenNhomDoiTuong) || x.TenNhomDoiTuong_KyTuDau.Contains(@TenNhomDoiTuong)).OrderBy(x => x.ID).ToList();
                }
                return lst;
            }
        }

        #region trinhpv load phân quyền
        public string getQuyen_NguoiDung(Guid ID_NguoiDung, Guid? ID_DonVi, string MaQuyen)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                classHT_Quyen classHTQuyen = new classHT_Quyen(db);

                string rt = string.Empty;
                HT_NguoiDung ngdung = classHTNguoiDung.Get(p => p.ID == ID_NguoiDung);
                if (ngdung.LaAdmin == false)
                {
                    var tb = from ht_quyen_nhom in db.HT_Quyen_Nhom
                             join ht_quyen in db.HT_Quyen on ht_quyen_nhom.MaQuyen equals ht_quyen.MaQuyen
                             join ht_nguoidung_nhom in db.HT_NguoiDung_Nhom on ht_quyen_nhom.ID_NhomNguoiDung equals ht_nguoidung_nhom.IDNhomNguoiDung
                             where ht_nguoidung_nhom.IDNguoiDung == ID_NguoiDung
                             & ht_quyen.MaQuyen == MaQuyen
                             & ht_nguoidung_nhom.ID_DonVi == ID_DonVi
                             select new
                             {
                                 ht_quyen_nhom.MaQuyen,
                                 ht_quyen.DuocSuDung
                             };
                    try
                    {
                        if (tb.FirstOrDefault().DuocSuDung == true)
                            rt = MaQuyen;
                        else
                            rt = "false";
                    }
                    catch
                    {
                        rt = "false";
                    }

                }
                else
                {
                    HT_Quyen quyen = classHTQuyen.Gets(p => p.MaQuyen == MaQuyen).FirstOrDefault();
                    if (quyen != null)
                    {
                        rt = quyen.MaQuyen;
                    }
                    else
                    {
                        rt = "false";
                    }
                }

                return rt;
            }
        }
        public string getQuyenXemGiaVon(Guid ID_NguoiDung, string MaQuyen)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                string rt = string.Empty;
                HT_NguoiDung ngdung = classHTNguoiDung.Get(p => p.ID == ID_NguoiDung);
                if (ngdung.LaAdmin == false)
                {
                    var tb = from nd in db.HT_NguoiDung
                             where nd.ID == ID_NguoiDung
                             select new
                             {
                                 nd.XemGiaVon
                             };
                    try
                    {
                        if (tb.FirstOrDefault().XemGiaVon == true)
                            rt = MaQuyen;
                        else
                            rt = "";
                    }
                    catch
                    {
                        rt = "";
                    }

                }
                else
                {
                    rt = MaQuyen;
                }
                return rt;
            }
        }
        #endregion
        #region Trinhpv Report_Open24_version2
        //Báo cáo nhân viên
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_TongHop(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_TongHopPRC> lst = new List<BaoCaoNhanVien_TongHopPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    List<Guid> lst_NHH = new List<Guid>();

                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                string a = array_BaoCaoNhanVien.MaNhanVien;
                if (array_BaoCaoNhanVien.MaNhanVien != null && array_BaoCaoNhanVien.MaNhanVien != "" && array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TongHop(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                    ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                    array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_TongHopPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_TongHopPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TongHop([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_TongHopPRC> lst = new List<BaoCaoNhanVien_TongHopPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                string a = array_BaoCaoNhanVien.MaNhanVien;
                if (array_BaoCaoNhanVien.MaNhanVien != null && array_BaoCaoNhanVien.MaNhanVien != "" && array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TongHop(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                    ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                    array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_TongHopPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoTongHopDanhSachNhanVien.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_TheoHopDong(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_HopDongPRC> lst = new List<BaoCaoNhanVien_HopDongPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoHopDong(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_HopDongPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_HopDongPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoHopDong([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_HopDongPRC> lst = new List<BaoCaoNhanVien_HopDongPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoHopDong(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_HopDongPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoLoaiHopDong.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_TheoBaoHiem(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_BaoHiemPRC> lst = new List<BaoCaoNhanVien_BaoHiemPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoBaoHiem(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_BaoHiemPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_BaoHiemPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoBaoHiem([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
                List<BaoCaoNhanVien_BaoHiemPRC> lst = new List<BaoCaoNhanVien_BaoHiemPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoBaoHiem(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_BaoHiemPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoBaoHiem.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_TheoTuoi(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_TuoiPRC> lst = new List<BaoCaoNhanVien_TuoiPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoTuoi(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                            ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                            array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai, array_BaoCaoNhanVien.Min, array_BaoCaoNhanVien.Max);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_TuoiPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_TuoiPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoDoTuoi([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_TuoiPRC> lst = new List<BaoCaoNhanVien_TuoiPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_TheoTuoi(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai, array_BaoCaoNhanVien.Min, array_BaoCaoNhanVien.Max);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_TuoiPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoDoTuoi.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_KhenThuong(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_KhenThuongPRC> lst = new List<BaoCaoNhanVien_KhenThuongPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_KhenThuong(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_KhenThuongPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_KhenThuongPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoKhenThuong([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_KhenThuongPRC> lst = new List<BaoCaoNhanVien_KhenThuongPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_KhenThuong(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_KhenThuongPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoKhenThuongKyLuat.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_LuongPhuCap(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_LuongPhuCapPRC> lst = new List<BaoCaoNhanVien_LuongPhuCapPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_LuongPhuCap(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_LuongPhuCapPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_LuongPhuCapPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoLuongPhuCap([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_LuongPhuCapPRC> lst = new List<BaoCaoNhanVien_LuongPhuCapPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_LuongPhuCap(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                        ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                        array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_LuongPhuCapPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoLuongPhuCap.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_MienGiamThue(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_MienGiamThuePRC> lst = new List<BaoCaoNhanVien_MienGiamThuePRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_MienGiamThue(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                            ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                            array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_MienGiamThuePRC> json = new JsonResultExampleTr<BaoCaoNhanVien_MienGiamThuePRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoMienGiamThue([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_MienGiamThuePRC> lst = new List<BaoCaoNhanVien_MienGiamThuePRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_MienGiamThue(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                            ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                            array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_MienGiamThuePRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoMienGiamThue.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_DaoTao(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_DaoTaoPRC> lst = new List<BaoCaoNhanVien_DaoTaoPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_DaoTao(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_DaoTaoPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_DaoTaoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoDaoTao([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_DaoTaoPRC> lst = new List<BaoCaoNhanVien_DaoTaoPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_DaoTao(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_DaoTaoPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoQuyTrinhDaoTao.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_QuaTrinhCongTac(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_QuaTrinhCongTacPRC> lst = new List<BaoCaoNhanVien_QuaTrinhCongTacPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_QuaTrinhCongTac(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_QuaTrinhCongTacPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_QuaTrinhCongTacPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoQuaTrinhCongTac([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_QuaTrinhCongTacPRC> lst = new List<BaoCaoNhanVien_QuaTrinhCongTacPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_QuaTrinhCongTac(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_QuaTrinhCongTacPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoQuaTrinhCongTac.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_ThongTinGiaDinh(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_GiaDinhPRC> lst = new List<BaoCaoNhanVien_GiaDinhPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_ThongTinGiaDinh(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_GiaDinhPRC> json = new JsonResultExampleTr<BaoCaoNhanVien_GiaDinhPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoGiaDinh([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_GiaDinhPRC> lst = new List<BaoCaoNhanVien_GiaDinhPRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_ThongTinGiaDinh(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_GiaDinhPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoThongTinGiaDinh.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhanVien_ThongTinSucKhoe(array_BaoCaoNhanVien array_BaoCaoNhanVien)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                List<BaoCaoNhanVien_SucKhoePRC> lst = new List<BaoCaoNhanVien_SucKhoePRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_ThongTinSucKhoe(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                int Rown = lst.Count();
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhanVien_SucKhoePRC> json = new JsonResultExampleTr<BaoCaoNhanVien_SucKhoePRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNV_TheoSucKhoe([FromBody] JObject data)
        {
            array_BaoCaoNhanVien array_BaoCaoNhanVien = data["objExcel"].ToObject<array_BaoCaoNhanVien>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhanVien reportNhanVien = new ClassReportNhanVien(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhanVien_SucKhoePRC> lst = new List<BaoCaoNhanVien_SucKhoePRC>();
                string TrangThai = "%";
                if (array_BaoCaoNhanVien.TrangThai.ToString() == "0")
                    TrangThai = "%0%";
                else if (array_BaoCaoNhanVien.TrangThai.ToString() == "1")
                    TrangThai = "%1%";
                string ID_PhongBan_SP = string.Empty;
                string ID_PhongBan_search = "%";
                if (array_BaoCaoNhanVien.ID_PhongBan != null)
                {
                    libNS_NhanVien.NhanSuService nhanSuService = new libNS_NhanVien.NhanSuService(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhanSuService.GetListIDPhongBanByID(array_BaoCaoNhanVien.ID_PhongBan, array_BaoCaoNhanVien.ID_DonVi);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_PhongBan_SP == string.Empty)
                            ID_PhongBan_SP = item.ToString();
                        else
                            ID_PhongBan_SP = ID_PhongBan_SP + "," + item.ToString();
                    }
                    ID_PhongBan_search = "%" + ID_PhongBan_SP + "%";
                }
                else
                    ID_PhongBan_SP = Guid.NewGuid().ToString();
                string GioiTinh = "%";
                if (array_BaoCaoNhanVien.GioiTinh.ToString() == "1")
                    GioiTinh = "%1%";
                else if (array_BaoCaoNhanVien.GioiTinh.ToString() == "0")
                    GioiTinh = "%0%";
                string LoaiDanToc_SP = string.Empty;
                string LoaiDanToc_search = "%";
                if (array_BaoCaoNhanVien.LoaiDanToc != null)
                {
                    LoaiDanToc_SP = array_BaoCaoNhanVien.LoaiDanToc;
                    LoaiDanToc_search = "%" + LoaiDanToc_SP + "%";
                }
                string MaNV_search = "%";
                string MaNV_TV = "%";
                if (array_BaoCaoNhanVien.MaNhanVien != null & array_BaoCaoNhanVien.MaNhanVien != "" & array_BaoCaoNhanVien.MaNhanVien != "null")
                {
                    MaNV_TV = "%" + array_BaoCaoNhanVien.MaNhanVien.Trim() + "%";
                    MaNV_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoNhanVien.MaNhanVien).ToLower() + "%";
                }
                lst = reportNhanVien.GetBaoCaoNhanVien_ThongTinSucKhoe(MaNV_search, MaNV_TV, array_BaoCaoNhanVien.ID_ChiNhanh, array_BaoCaoNhanVien.timeCreate_Start, array_BaoCaoNhanVien.timeCreate_End,
                                ID_PhongBan_search, ID_PhongBan_SP, GioiTinh, array_BaoCaoNhanVien.LoaiHopDong, array_BaoCaoNhanVien.timeBirthday_Start, array_BaoCaoNhanVien.timeBirthday_End,
                                array_BaoCaoNhanVien.LoaiChinhTri, array_BaoCaoNhanVien.LoaiBaoHiem, LoaiDanToc_search, LoaiDanToc_SP, TrangThai);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhanVien_SucKhoePRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhanVien/Teamplate_BaoCaoNhanVienTheoThongTinSucKhoe.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoNhanVien.TenChiNhanh, array_BaoCaoNhanVien.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoNhanVien.columnsHide, lstDataCell);
                return response;
            }
        }
        //Báo cáo gói dịch vụ
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_SoDuChiTiet(array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_SoDuChiTietPRC> lst = new List<BaoCaoGoiDichVu_SoDuChiTietPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_SoDuChiTiet(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd, idChiNhanhs,
                    LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP, array_BaoCaoGoiDichVu.ID_NguoiDung);
                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuong);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                double SoLuongSuDung = lst.Sum(x => x.SoLuongSuDung);
                double? gtriSuDung = lst.Sum(x => x.GiaTriSD ?? 0);
                double? gtriConLai = lst.Sum(x => x.GiaTriConLai);
                double GiaVon = lst.Sum(x => x.GiaVon);
                double SoLuongConLai = lst.Sum(x => x.SoLuongConLai);
                int lstPages = getNumber_Page(Rown, 10);

                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a5 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                    a6 = Math.Round(SoLuongSuDung, 3, MidpointRounding.ToEven),
                    a7 = Math.Round(GiaVon, 0, MidpointRounding.ToEven),
                    a8 = Math.Round(SoLuongConLai, 3, MidpointRounding.ToEven),
                    GtriSuDung = gtriSuDung,
                    GtriConLai = gtriConLai,
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_SoDuChiTiet([FromBody] JObject data)
        {
            array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu = data["objExcel"].ToObject<array_BaoCaoGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_SoDuChiTietPRC> lst = new List<BaoCaoGoiDichVu_SoDuChiTietPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_SoDuChiTiet(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd, idChiNhanhs,
                    LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP, array_BaoCaoGoiDichVu.ID_NguoiDung);
                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_SoDuChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_BaoCaoChiTietSoDuGoiDichVu.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoGoiDichVu.TenChiNhanh, array_BaoCaoGoiDichVu.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoGoiDichVu.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_SoDuTongHop(array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_SoDuTongHopPRC> lst = new List<BaoCaoGoiDichVu_SoDuTongHopPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                switch (array_BaoCaoGoiDichVu.ThoiHanSuDung)
                {
                    case 2:
                        ThoiHan = "%1%";
                        break;
                    case 3:
                        ThoiHan = "%0%";
                        break;
                }

                switch (array_BaoCaoGoiDichVu.TinhTrang)
                {
                    case 2:
                        TheoDoi = "%1%";
                        TrangThai = "%0%";
                        break;
                    case 3:
                        TheoDoi = "%0%";
                        break;
                    case 4:
                        TrangThai = "%1%";
                        break;
                }

                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_SoDuTongHop(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP, array_BaoCaoGoiDichVu.ID_NguoiDung);
                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuong);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                double SoLuongSuDung = lst.Sum(x => x.SoLuongSuDung);
                double GiaVon = lst.Sum(x => x.GiaVon);
                double SoLuongConLai = lst.Sum(x => x.SoLuongConLai);
                double? gtriMua = lst.Sum(x => x.GiaTriMua);
                double? gtriSuDung = lst.Sum(x => x.GiaTriSD);
                double? gtriConLai = lst.Sum(x => x.GiaTriConLai);
                double? giamGiaHD = lst.Sum(x => x.GiamGiaHD);
                int lstPages = getNumber_Page(Rown, 10);

                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongSuDung, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(GiaVon, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(SoLuongConLai, 3, MidpointRounding.ToEven),
                    GtriSuDung = gtriSuDung,
                    GtriConLai = gtriConLai,
                    GiamGiaHD = giamGiaHD,
                    GiaTriMua = gtriMua,
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_SoDuTongHop([FromBody] JObject data)
        {
            array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu = data["objExcel"].ToObject<array_BaoCaoGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_SoDuTongHopPRC> lst = new List<BaoCaoGoiDichVu_SoDuTongHopPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_SoDuTongHop(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP, array_BaoCaoGoiDichVu.ID_NguoiDung);
                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_SoDuTongHopPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_BaoCaoTongHopSoDuGoiDichVu.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoGoiDichVu.TenChiNhanh, array_BaoCaoGoiDichVu.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoGoiDichVu.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_NhatKySuDungChiTiet(array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_NhatKySuDungChiTietPRC> lst = new List<BaoCaoGoiDichVu_NhatKySuDungChiTietPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string LaHH_search = "%%";
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_NhatKySuDungChiTiet(array_BaoCaoGoiDichVu.MaHangHoa, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd, idChiNhanhs,
                    LaHH_search, TheoDoi, TrangThai, ThoiHan, array_BaoCaoGoiDichVu.ID_NhomHang);
                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuong);
                int lstPages = getNumber_Page(Rown, 10);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    TongGiaTriSD = lst.Sum(x => x.GiaTriSD),
                    TongTienVon = lst.Sum(x => x.TienVon),
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_NhatKySuDungChiTiet([FromBody] JObject data)
        {
            array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu = data["objExcel"].ToObject<array_BaoCaoGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_NhatKySuDungChiTietPRC> lst = new List<BaoCaoGoiDichVu_NhatKySuDungChiTietPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string LaHH_search = "%%";
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_NhatKySuDungChiTiet(array_BaoCaoGoiDichVu.MaHangHoa, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd, idChiNhanhs,
                    LaHH_search, TheoDoi, TrangThai, ThoiHan, array_BaoCaoGoiDichVu.ID_NhomHang);
                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_NhatKySuDungChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_ChiTietNhatKySuDungGoiDichVu.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoGoiDichVu.TenChiNhanh, array_BaoCaoGoiDichVu.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoGoiDichVu.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_NhatKySuDungTongHop(Param_ReportGoiDichVu param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_NhatKySuDungTongHopPRC> lst = reportGoiDichVu.GetBaoCaoDichVu_NhatKySuDungTongHop(param);

                int Rown = lst.Count();
                double SoLuongMua = lst.Sum(x => x.SoLuongMua);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double SoLuongSuDung = lst.Sum(x => x.SoLuongSuDung);
                double SoLuongConLai = lst.Sum(x => x.SoLuongConLai);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoGoiDichVu_NhatKySuDungTongHopPRC> json = new JsonResultExampleTr<BaoCaoGoiDichVu_NhatKySuDungTongHopPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongMua, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongSuDung, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(SoLuongConLai, 3, MidpointRounding.ToEven)
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_NhatKySuDungTongHop([FromBody] JObject data)
        {
            Param_ReportGoiDichVu param = data["objExcel"].ToObject<Param_ReportGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_NhatKySuDungTongHopPRC> lst = reportGoiDichVu.GetBaoCaoDichVu_NhatKySuDungTongHop(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_NhatKySuDungTongHopPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_TongHopNhatKySuDungGoiDichVu.xlsx");
                string columHide = string.Empty;
                if (param.ColumnHide != null)
                {
                    columHide = string.Join("_", param.ColumnHide);
                }
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.ReportBranch, param.ReportTime);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, columHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_TonChuaSuDung(array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_TonChuaSuDungPRC> lst = new List<BaoCaoGoiDichVu_TonChuaSuDungPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_TonChuaSuDung(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP);
                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuongBan);
                double GiaTriBan = lst.Sum(x => x.GiaTriBan);
                double SoLuongSuDung = lst.Sum(x => x.SoLuongSuDung);
                double GiaTriSuDung = lst.Sum(x => x.GiaTriSuDung);
                double SoLuongConLai = lst.Sum(x => x.SoLuongConLai);
                double GiaTriConLai = lst.Sum(x => x.GiaTriConLai);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoGoiDichVu_TonChuaSuDungPRC> json = new JsonResultExampleTr<BaoCaoGoiDichVu_TonChuaSuDungPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriBan, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongSuDung, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(GiaTriSuDung, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(SoLuongConLai, 3, MidpointRounding.ToEven),
                    a8 = Math.Round(GiaTriConLai, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_TonChuaSuDung([FromBody] JObject data)
        {
            array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu = data["objExcel"].ToObject<array_BaoCaoGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_TonChuaSuDungPRC> lst = new List<BaoCaoGoiDichVu_TonChuaSuDungPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_TonChuaSuDung(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP);
                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_TonChuaSuDungPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                //excel.Columns.Remove("TenDonViTinh");
                //excel.Columns.Remove("TenLoHang");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_TonDichVuChuaSuDung.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoGoiDichVu.TenChiNhanh, array_BaoCaoGoiDichVu.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, array_BaoCaoGoiDichVu.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDichVu_NhapXuatTon(array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                List<BaoCaoGoiDichVu_NhapXuatTonPRC> lst = new List<BaoCaoGoiDichVu_NhapXuatTonPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_NhapXuatTon(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP);
                int Rown = lst.Count();
                double SoLuongConLaiDK = lst.Sum(x => x.SoLuongConLaiDK);
                double GiaTriConLaiDK = lst.Sum(x => x.GiaTriConLaiDK);
                double SoLuongBanGK = lst.Sum(x => x.SoLuongBanGK);
                double GiaTriBanGK = lst.Sum(x => x.GiaTriBanGK);
                double SoLuongTraGK = lst.Sum(x => x.SoLuongTraGK);
                double GiaTriTraGK = lst.Sum(x => x.GiaTriTraGK);
                double SoLuongSuDungGK = lst.Sum(x => x.SoLuongSuDungGK);
                double GiaTriSuDungGK = lst.Sum(x => x.GiaTriSuDungGK);
                double SoLuongConLaiCK = lst.Sum(x => x.SoLuongConLaiCK);
                double GiaTriConLaiCK = lst.Sum(x => x.GiaTriConLaiCK);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoGoiDichVu_NhapXuatTonPRC> json = new JsonResultExampleTr<BaoCaoGoiDichVu_NhapXuatTonPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongConLaiDK, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriConLaiDK, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongBanGK, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriBanGK, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongTraGK, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(GiaTriTraGK, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(SoLuongSuDungGK, 3, MidpointRounding.ToEven),
                    a8 = Math.Round(GiaTriSuDungGK, 0, MidpointRounding.ToEven),
                    a9 = Math.Round(SoLuongConLaiCK, 3, MidpointRounding.ToEven),
                    a10 = Math.Round(GiaTriConLaiCK, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCGDV_NhapXuatTon([FromBody] JObject data)
        {
            array_BaoCaoGoiDichVu array_BaoCaoGoiDichVu = data["objExcel"].ToObject<array_BaoCaoGoiDichVu>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportGoiDichVu reportGoiDichVu = new ClassReportGoiDichVu(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoGoiDichVu_NhapXuatTonPRC> lst = new List<BaoCaoGoiDichVu_NhapXuatTonPRC>();
                string TheoDoi = "%%";
                string TrangThai = "%%";
                string ThoiHan = "%%";
                if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 2)
                    ThoiHan = "%1%";
                else if (array_BaoCaoGoiDichVu.ThoiHanSuDung == 3)
                    ThoiHan = "%0%";
                if (array_BaoCaoGoiDichVu.TinhTrang == 2)
                {
                    TheoDoi = "%1%";
                    TrangThai = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 3)
                {
                    TheoDoi = "%0%";
                }
                else if (array_BaoCaoGoiDichVu.TinhTrang == 4)
                {
                    TrangThai = "%1%";
                }
                string ID_NhomHang_SP = string.Empty;
                string ID_NhomHang_search = "%%";
                if (array_BaoCaoGoiDichVu.ID_NhomHang != null)
                {
                    libDM_NhomHangHoa.classDM_NhomHangHoa nhomHangHoa = new libDM_NhomHangHoa.classDM_NhomHangHoa(db);
                    List<Guid> lst_NHH = new List<Guid>();
                    lst_NHH = nhomHangHoa.GetListIDNhomHangByID(array_BaoCaoGoiDichVu.ID_NhomHang);
                    foreach (var item in lst_NHH)
                    {
                        if (ID_NhomHang_SP == string.Empty)
                            ID_NhomHang_SP = item.ToString();
                        else
                            ID_NhomHang_SP = ID_NhomHang_SP + "," + item.ToString();
                    }
                    ID_NhomHang_search = "%" + ID_NhomHang_SP + "%";
                }
                else
                    ID_NhomHang_SP = Guid.NewGuid().ToString();
                string maHD_search = "%%";
                string LaHH_search = "%%";
                string MaHH_search = "%%";
                string MaKH_search = "%%";
                string MaKH_TV = "%%";
                if (array_BaoCaoGoiDichVu.MaHangHoa != null & array_BaoCaoGoiDichVu.MaHangHoa != "" & array_BaoCaoGoiDichVu.MaHangHoa != "null")
                {
                    maHD_search = "%" + array_BaoCaoGoiDichVu.MaHangHoa.Trim() + "%";
                    MaHH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaHangHoa).ToLower() + "%";
                }
                if (array_BaoCaoGoiDichVu.MaKhachHang != null & array_BaoCaoGoiDichVu.MaKhachHang != "" & array_BaoCaoGoiDichVu.MaKhachHang != "null")
                {
                    MaKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoGoiDichVu.MaKhachHang).ToLower() + "%";
                    MaKH_TV = "%" + array_BaoCaoGoiDichVu.MaKhachHang.Trim() + "%";
                }
                if (array_BaoCaoGoiDichVu.LaHangHoa == 0)
                    LaHH_search = "0";
                else if (array_BaoCaoGoiDichVu.LaHangHoa == 1)
                    LaHH_search = "1";
                var idChiNhanhs = string.Join(",", array_BaoCaoGoiDichVu.lstIDChiNhanh);
                lst = reportGoiDichVu.GetBaoCaoDichVu_NhapXuatTon(maHD_search, MaHH_search, MaKH_search, MaKH_TV, array_BaoCaoGoiDichVu.timeStart, array_BaoCaoGoiDichVu.timeEnd,
                    idChiNhanhs, LaHH_search, TheoDoi, TrangThai, ThoiHan, ID_NhomHang_search, ID_NhomHang_SP);
                DataTable excel = classOffice.ToDataTable<BaoCaoGoiDichVu_NhapXuatTonPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                //excel.Columns.Remove("TenDonViTinh");
                //excel.Columns.Remove("TenLoHang");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoGoiDichVu/Teamplate_BaoCaoNhapXuatTonDichVu.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(array_BaoCaoGoiDichVu.TenChiNhanh, array_BaoCaoGoiDichVu.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, array_BaoCaoGoiDichVu.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_TongHop(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_TongHopPRC> lst = report.GetBaoCaoBanHang_TongHopPRC(param);

                int Rown = 0;
                double SoLuong = 0, ThanhTien = 0;
                double GiamGiaHD = 0, sumGiamtruBaoHiem = 0;
                double LaiLo = 0, TienVon = 0;
                double? TongDoanhThuThuan = 0, SumTienThue = 0, sumChiPhi = 0;
                if (lst.Count() > 0)
                {
                    Rown = Convert.ToInt32(lst.FirstOrDefault().Rowns);
                    SoLuong = lst.FirstOrDefault().TongSoLuong;
                    ThanhTien = lst.FirstOrDefault().TongThanhTien;
                    TienVon = lst.FirstOrDefault().TongTienVon;
                    GiamGiaHD = lst.FirstOrDefault().TongGiamGiaHD;
                    LaiLo = lst.FirstOrDefault().TongLaiLo;
                    TongDoanhThuThuan = lst.FirstOrDefault().TongDoanhThuThuan;
                    SumTienThue = lst.FirstOrDefault().SumTienThue;
                    sumChiPhi = lst.FirstOrDefault().TongChiPhi;
                    sumGiamtruBaoHiem = lst.FirstOrDefault().TongGiamTruBaoHiem ?? 0;
                }
                int lstPages = getNumber_Page(Rown, param.pageSize);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(TienVon, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(LaiLo, 0, MidpointRounding.ToEven),
                    TongDoanhThuThuan = Math.Round(TongDoanhThuThuan ?? 0, 0, MidpointRounding.ToEven),
                    SumTienThue = Math.Round(SumTienThue ?? 0, 0, MidpointRounding.ToEven),
                    SumChiPhi = Math.Round(sumChiPhi ?? 0, 0, MidpointRounding.ToEven),
                    TongGiamTruBaoHiem = sumGiamtruBaoHiem
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_TongHop([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoBanHang_TongHopPRC> lst = report.GetBaoCaoBanHang_TongHopPRC(param);


                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_TongHopPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");

                excel.Columns.Remove("Rowns");
                excel.Columns.Remove("TongSoLuong");
                excel.Columns.Remove("TongThanhTien");
                excel.Columns.Remove("TongTienVon");
                excel.Columns.Remove("TongGiamGiaHD");
                excel.Columns.Remove("TongChiPhi");
                excel.Columns.Remove("TongLaiLo");
                excel.Columns.Remove("SumTienThue");
                excel.Columns.Remove("TongDoanhThuThuan");
                excel.Columns.Remove("TongChietKhau");
                excel.Columns.Remove("TongTienTruocCK");
                excel.Columns.Remove("TongGiamTruBaoHiem");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangTongHop.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_ChiTiet(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_ChiTietPRC> lst = report.GetBaoCaoBanHang_ChiTietPRC(param);

                int Rown = 0;
                double SoLuong = 0, ThanhTien = 0;
                double GiamGiaHD = 0, sumGiamtruBaoHiem = 0;
                double LaiLo = 0, TienVon = 0;
                double? TongDoanhThuThuan = 0, SumTienThue = 0, tongChiPhi = 0;
                if (lst.Count() > 0)
                {
                    Rown = Convert.ToInt32(lst.FirstOrDefault().Rowns);
                    SoLuong = lst.FirstOrDefault().TongSoLuong;
                    ThanhTien = lst.FirstOrDefault().TongThanhTien;
                    TienVon = lst.FirstOrDefault().TongTienVon;
                    GiamGiaHD = lst.FirstOrDefault().TongGiamGiaHD;
                    LaiLo = lst.FirstOrDefault().TongLaiLo;
                    TongDoanhThuThuan = lst.FirstOrDefault().DoanhThuThuan;
                    SumTienThue = lst.FirstOrDefault().TongTienThue;
                    tongChiPhi = lst.FirstOrDefault().TongChiPhi;
                    sumGiamtruBaoHiem = lst.FirstOrDefault().TongGiamTruBaoHiem ?? 0;
                }
                int lstPages = getNumber_Page(Rown, param.pageSize);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(TienVon, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(LaiLo, 0, MidpointRounding.ToEven),
                    TongDoanhThuThuan = Math.Round(TongDoanhThuThuan ?? 0, 0, MidpointRounding.ToEven),
                    SumTienThue = Math.Round(SumTienThue ?? 0, 0, MidpointRounding.ToEven),
                    SumChiPhi = Math.Round(tongChiPhi ?? 0, 0, MidpointRounding.ToEven),
                    TongGiamTruBaoHiem = sumGiamtruBaoHiem
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_ChiTiet([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();

                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_ChiTietPRC> lst = report.GetBaoCaoBanHang_ChiTietPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_ChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                excel.Columns.Remove("Rowns");
                excel.Columns.Remove("TongSoLuong");
                excel.Columns.Remove("TongThanhTien");
                excel.Columns.Remove("TongTienVon");
                excel.Columns.Remove("TongChiPhi");
                excel.Columns.Remove("TongGiamGiaHD");
                excel.Columns.Remove("TongLaiLo");
                excel.Columns.Remove("TongTienThue");
                excel.Columns.Remove("GioiTinh");
                excel.Columns.Remove("DoanhThuThuan");
                excel.Columns.Remove("LoaiHoaDon");
                excel.Columns.Remove("TongChietKhau");
                excel.Columns.Remove("TongTienTruocCK");
                excel.Columns.Remove("TongGiamTruBaoHiem");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangChiTiet.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_NhomHang(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_NhomHangPRC> lst = report.GetBaoCaoBanHang_NhomHangPRC(param);

                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double? ThanhTien = lst.Sum(x => x.ThanhTien);
                double TienVon = lst.Sum(x => x.TienVon);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double LaiLo = lst.Sum(x => x.LaiLo);
                double? TongDoanhThuThuan = lst.Sum(x => x.DoanhThu);
                double? SumTienThue = lst.Sum(x => x.TienThue);
                double? sumChiPhi = lst.Sum(x => x.ChiPhi);
                double? sumCK = lst.Sum(x => x.TienChietKhau);
                double? sumThanhTientruocCK = lst.Sum(x => x.ThanhTienTruocCK);
                double? sumGiamTruBaoHiem = lst.Sum(x => x.GiamTruThanhToanBaoHiem);

                int lstPages = getNumber_Page(Rown, param.pageSize);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = ThanhTien,
                    a3 = Math.Round(TienVon, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(LaiLo, 0, MidpointRounding.ToEven),
                    TongDoanhThuThuan = Math.Round(TongDoanhThuThuan ?? 0, 0, MidpointRounding.ToEven),
                    SumTienThue = Math.Round(SumTienThue ?? 0, 0, MidpointRounding.ToEven),
                    SumChiPhi = Math.Round(sumChiPhi ?? 0, 0, MidpointRounding.ToEven),
                    SumChietKhau = sumCK,
                    SumThanhTientruocCK = sumThanhTientruocCK,
                    TongGiamTruBaoHiem = sumGiamTruBaoHiem,
                });
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKhachHang_TanSuat(ParamSearchReportKhachHang lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    List<BaoCaoBanHang_TheoKhachHangTanSuat> lst = report.BaoCaoKhachHang_TanSuat(lstParam);
                    return Json(new { res = true, data = lst });
                }
                catch (Exception ex)
                {
                    return Json(new { res = false, mes = ex });
                }
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BaoCaoKhachHangTanSuat(ParamSearchReportKhachHang lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                    List<BaoCaoBanHang_TheoKhachHangTanSuat> lst = report.BaoCaoKhachHang_TanSuat(lstParam);
                    DataTable excel = classOffice.ToDataTable(lst);
                    excel.Columns.Remove("ID_KhachHang");
                    excel.Columns.Remove("TotalRow");
                    excel.Columns.Remove("TotalPage");
                    excel.Columns.Remove("TongSoLanDen");
                    excel.Columns.Remove("TongMua");
                    excel.Columns.Remove("TongTra");
                    excel.Columns.Remove("TongDoanhThu");

                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoKhachHangTanSuat.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(lstParam.Export_ChiNhanh, lstParam.Export_Time);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, lstParam.columnsHide, lstDataCell);
                    return response;
                }
                catch (Exception ex)
                {
                    CookieStore.WriteLog(string.Concat("Export_BaoCaoKhachHangTanSuat ", ex));
                    return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
                }
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetNhatKyGiaoDich_ofKhachHang(ParamSearchReportKhachHang lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    List<BCTanSuat_NhatKyGiaoDich> lst = report.GetNhatKyGiaoDich_ofKhachHang(lstParam);

                    var count = lst.Count() > 0 ? (int)lst[0].TotalRow : 0;
                    int page = 0;
                    var pagenow = lstParam.CurrentPage + 1;
                    var listpage = GetListPage(count, lstParam.PageSize, pagenow, ref page);

                    if (lst.Count() > 0)
                    {
                        var itFirst = lst[0];
                        return Json(new
                        {
                            res = true,
                            data = lst,
                            itFirst.SumTienHang,
                            itFirst.SumTienThue,
                            itFirst.SumGiamGia,
                            itFirst.SumChiPhi,
                            itFirst.SumChietKhau,
                            itFirst.SumPhaiThanhToan,
                            itFirst.SumTienMat,
                            itFirst.SumTienGui,
                            itFirst.SumThuTuThe,
                            itFirst.SumDaThanhToan,

                            itFirst.TotalPage,
                            ListPage = listpage,
                            PageView_Text = string.Concat("Hiển thị ", (lstParam.CurrentPage * lstParam.PageSize + 1), " - ", (lstParam.CurrentPage * lstParam.PageSize + lst.Count),
                            " trên tổng số ", count, " bản ghi "),
                            VisiblePrev = pagenow > 3 && page > 5,
                            VisibleNext = pagenow < page - 2 && page > 5,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            data = lst,
                            SumTienHang = 0,
                            SumTienThue = 0,
                            SumGiamGia = 0,
                            SumChiPhi = 0,
                            SumChietKhau = 0,
                            SumPhaiThanhToan = 0,
                            SumTienMat = 0,
                            SumTienGui = 0,
                            SumThuTuThe = 0,
                            SumDaThanhToan = 0,

                            TotalPage = 0,
                            ListPage = listpage,
                            PageView_Text = "",
                            VisiblePrev = false,
                            VisibleNext = false,
                        });
                    }
                }
                catch (Exception ex)
                {
                    return Json(new { res = false, mes = ex });
                }
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_NhatKyGiaoDichKhachHang(ParamSearchReportKhachHang lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);

                    List<BCTanSuat_NhatKyGiaoDich> lst = report.GetNhatKyGiaoDich_ofKhachHang(lstParam);
                    DataTable excel = classOffice.ToDataTable(lst);
                    excel.Columns.Remove("ID");
                    excel.Columns.Remove("TenDonVi");
                    excel.Columns.Remove("TongTienThue");
                    excel.Columns.Remove("TongChiPhi");
                    excel.Columns.Remove("TongChietKhau");
                    excel.Columns.Remove("TienMat");
                    excel.Columns.Remove("TienGui");
                    excel.Columns.Remove("ThuTuThe");
                    excel.Columns.Remove("TotalRow");
                    excel.Columns.Remove("TotalPage");
                    excel.Columns.Remove("SumTienHang");
                    excel.Columns.Remove("SumTienThue");
                    excel.Columns.Remove("SumGiamGia");
                    excel.Columns.Remove("SumChiPhi");
                    excel.Columns.Remove("SumChietKhau");
                    excel.Columns.Remove("SumPhaiThanhToan");
                    excel.Columns.Remove("SumTienMat");
                    excel.Columns.Remove("SumTienGui");
                    excel.Columns.Remove("SumThuTuThe");
                    excel.Columns.Remove("SumDaThanhToan");

                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoKhachHang_NhatKyGiaoDich.xlsx");
                    var fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCaoKhachHang_NhatKyGiaoDich.xlsx");
                    fileSave = classOffice.createFolder_Download(fileSave);
                    classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, true, null, lstParam.Export_Time, lstParam.Export_ChiNhanh);

                    var index = fileSave.IndexOf(@"\Template");
                    fileSave = "~" + fileSave.Substring(index, fileSave.Length - index);
                    fileSave = fileSave.Replace(@"\", "/");
                    return fileSave;
                }
                catch (Exception ex)
                {
                    CookieStore.WriteLog(string.Concat("Export_NhatKyGiaoDichKhachHang ", ex));
                    return "";
                }
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_TheoNhomHang([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHang_NhomHangPRC> lst = report.GetBaoCaoBanHang_NhomHangPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_NhomHangPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangTheoNhomHang.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_TheoKhachHang(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    db.Database.CommandTimeout = 60 * 60;
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    List<BaoCaoBanHang_TheoKhachHangPRC> lst = report.GetBaoCaoBanHang_TheoKhachHangPRC(param);

                    int Rown = lst.Count();
                    double SoLuongMua = lst.Sum(x => x.SoLuongMua ?? 0);
                    double SoLuongTra = lst.Sum(x => x.SoLuongTra ?? 0);
                    double SoLuong = SoLuongMua - SoLuongTra;
                    double GiaTriMua = lst.Sum(x => x.GiaTriMua ?? 0);
                    double GiaTriTra = lst.Sum(x => x.GiaTriTra ?? 0);
                    double DoanhThu = GiaTriMua - GiaTriTra;
                    double TienVon = lst.Sum(x => x.TienVon);
                    double TongTienThue = lst.Sum(x => x.TongTienThue ?? 0);
                    double? chiphi = lst.Sum(x => x.ChiPhi ?? 0);
                    double LaiLo = lst.Sum(x => x.LaiLo);

                    int lstPages = getNumber_Page(Rown, param.pageSize);
                    return Json(new
                    {
                        res = true,
                        LstData = lst,
                        Rowcount = Rown,
                        numberPage = lstPages,
                        SoLuongMua,
                        GiaTriMua,
                        SoLuongTra,
                        GiaTriTra,
                        SoLuong,
                        DoanhThu,
                        TongTienThue,
                        TienVon,
                        LaiLo,
                        SumChiPhi = chiphi,
                    });
                }
                catch (Exception e)
                {
                    return Json(new
                    {
                        res = false,
                        mes = e.Message,
                    });
                }
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_TheoKhachHang([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHang_TheoKhachHangPRC> lst = report.GetBaoCaoBanHang_TheoKhachHangPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_TheoKhachHangPRC>(lst);
                excel.Columns.Remove("ID_KhachHang");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangTheoKhachHang.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [HttpGet, HttpPost] // todo
        public IHttpActionResult BaoCaoBanHangChiTiet_TheoKhachHang(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {


                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportBanHang report = new ClassReportBanHang(db);
                    List<BaoCaoBanHangChiTiet_TheoKhachHangPRC> lst = lst = report.GetBaoCaoBanHangChiTiet_TheoKhachHangPRC(param);

                    int Rown = lst.Count();
                    List<ListLHPages> lstPages = getAllPage<BaoCaoBanHangChiTiet_TheoKhachHangPRC>(lst, 10);
                    return Json(new
                    {
                        res = true,
                        LstData = lst,
                        Rowcount = Rown,
                        LstPageNumber = lstPages,
                    });
                }
                catch (Exception e)
                {
                    return Json(new { res = false, mes = e.Message });
                }
            }
        }
        [HttpGet, HttpPost]
        public HttpResponseMessage Export_BCBHCT_TheoKhachHang([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHangChiTiet_TheoKhachHangPRC> lst = report.GetBaoCaoBanHangChiTiet_TheoKhachHangPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHangChiTiet_TheoKhachHangPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangChiTietTheoKhachHang.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                lstDataCell.Add(new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 4, CellValue = param.chitietBC });
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_TheoNhanVien(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_TheoNhanVienPRC> lst = report.GetBaoCaoBanHang_TheoNhanVienPRC(param);
                double? TongDoanhThuThuan = lst.Sum(x => x.DoanhThu);
                double? SumTienThue = lst.Sum(x => x.TienThue);

                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuongBan);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                double TienVon = lst.Sum(x => x.TienVon);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double LaiLo = lst.Sum(x => x.LaiLo);
                double? chiphi = lst.Sum(x => x.ChiPhi);
                double? sumGiamTruBaoHiem = lst.Sum(x => x.GiamTruThanhToanBaoHiem);
                int lstPages = getNumber_Page(Rown, param.pageSize);

                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(TienVon, 0, MidpointRounding.ToEven),
                    a6 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(LaiLo, 0, MidpointRounding.ToEven),
                    TongDoanhThuThuan = Math.Round(TongDoanhThuThuan ?? 0, 0, MidpointRounding.ToEven),
                    SumTienThue = Math.Round(SumTienThue ?? 0, 0, MidpointRounding.ToEven),
                    SumChiPhi = Math.Round(chiphi ?? 0, 0, MidpointRounding.ToEven),
                    TongGiamTruBaoHiem = sumGiamTruBaoHiem,
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_TheoNhanVien([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHang_TheoNhanVienPRC> lst = report.GetBaoCaoBanHang_TheoNhanVienPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_TheoNhanVienPRC>(lst);
                excel.Columns.Remove("ID_NhanVien");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangTheoNhanVien.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [HttpGet, HttpPost]
        public IHttpActionResult BaoCaoBanHangChiTiet_TheoNhanVien(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHangChiTiet_TheoNhanVienPRC> lst = report.GetBaoCaoBanHangChiTiet_TheoNhanVienPRC(param);

                int Rown = lst.Count();
                List<ListLHPages> lstPages = getAllPage<BaoCaoBanHangChiTiet_TheoNhanVienPRC>(lst, 10);
                JsonResultExampleTr<BaoCaoBanHangChiTiet_TheoNhanVienPRC> json = new JsonResultExampleTr<BaoCaoBanHangChiTiet_TheoNhanVienPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    LstPageNumber = lstPages,
                };
                return Json(json);
            }
        }
        [HttpGet, HttpPost]
        public HttpResponseMessage Export_BCBHCT_TheoNhanVien([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoBanHangChiTiet_TheoNhanVienPRC> lst = report.GetBaoCaoBanHangChiTiet_TheoNhanVienPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHangChiTiet_TheoNhanVienPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                excel.Columns.Remove("DoanhThu");
                excel.Columns.Remove("TienThue");

                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangChiTietTheoNhanVien.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                lstDataCell.Add(new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 6, CellValue = param.chitietBC });
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_HangTraLai(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_HangTraLaiPRC> lst = report.GetBaoCaoBanHang_HangTraLaiPRC(param);

                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                int lstPages = getNumber_Page(Rown, param.pageSize);
                JsonResultExampleTr<BaoCaoBanHang_HangTraLaiPRC> json = new JsonResultExampleTr<BaoCaoBanHang_HangTraLaiPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_HangTraLai([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHang_HangTraLaiPRC> lst = report.GetBaoCaoBanHang_HangTraLaiPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_HangTraLaiPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("LoaiHoaDon");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHang_HangTraLai.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoHangKhuyenMai(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoHangKhuyenMai> lst = report.SP_BaoCaoHangKhuyenMai(param);

                double soluong = lst.Sum(x => x.SoLuong);
                double giatrikm = lst.Sum(x => x.GiaTriKM);

                if (lst.Count() > 0)
                {
                    var firstRow = lst[0];
                    var lstGr = lst.GroupBy(x => new
                    {
                        //x.MaKhuyenMai,
                        //x.TenKhuyenMai,
                        //x.sHinhThuc,
                        x.MaHoaDon,
                        x.NgayLapHoaDon,
                        x.TongTienHang,
                        x.MaDoiTuong,
                        x.TenDoiTuong,
                        x.NguoiTao,
                        x.TenNhanVien
                    }).Select(x => new
                    {
                        //x.Key.MaKhuyenMai,
                        //x.Key.TenKhuyenMai,
                        //x.Key.sHinhThuc,
                        x.Key.MaHoaDon,
                        x.Key.NgayLapHoaDon,
                        x.Key.TongTienHang,
                        x.Key.MaDoiTuong,
                        x.Key.TenDoiTuong,
                        x.Key.NguoiTao,
                        x.Key.TenNhanVien,
                        lstDetail = x,
                    });

                    int rows = lstGr.Count();
                    double lstPages = Math.Ceiling(rows * 1.0 / param.pageSize);
                    return Json(new
                    {
                        LstData = lstGr,
                        Rowcount = rows,
                        numberPage = lstPages,
                        TongDoanhThu = lstGr.Sum(x => x.TongTienHang),
                        TongSoLuong = Math.Round(soluong, 3, MidpointRounding.ToEven),
                        TongGiatriKM = Math.Round(giatrikm, 0, MidpointRounding.ToEven),
                    });
                }
                else
                {
                    return Json(new
                    {
                        res = true,
                        LstData = new List<BaoCaoHangKhuyenMai>(),
                        Rowcount = 0,
                        numberPage = 0,
                        TongDoanhThu = 0,
                        TongSoLuong = 0,
                        TongGiatriKM = 0
                    });
                }
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BaoCaoHangKhuyenMai([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoHangKhuyenMai> lst = report.SP_BaoCaoHangKhuyenMai(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoHangKhuyenMai>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoHangKhuyenMai.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoBanHang_LoiNhuan(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportBanHang report = new ClassReportBanHang(db);
                List<BaoCaoBanHang_LoiNhuanPRC> lst = report.GetBaoCaoBanHang_LoiNhuanPRC(param);

                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuongBan);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double SoLuongTra = lst.Sum(x => x.SoLuongTra);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                double DoanhThuThuan = lst.Sum(x => x.DoanhThuThuan);
                double TienVon = lst.Sum(x => x.TienVon);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double LaiLo = lst.Sum(x => x.LaiLo);
                double? tongThue = lst.Sum(x => x.TienThue);
                double? tongChiPhi = lst.Sum(x => x.ChiPhi);
                double? sumGiamTruBaoHiem = lst.Sum(x => x.GiamTruThanhToanBaoHiem);
                double TySuat = (LaiLo / DoanhThuThuan) * 100;
                int lstPages = getNumber_Page(Rown, param.pageSize);

                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongTra, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a6 = Math.Round(DoanhThuThuan, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(TienVon, 0, MidpointRounding.ToEven),
                    a8 = Math.Round(LaiLo, 0, MidpointRounding.ToEven),
                    a9 = Math.Round(TySuat, 2, MidpointRounding.ToEven),
                    TongTienThue = tongThue,
                    SumChiPhi = tongChiPhi,
                    TongGiamTruBaoHiem = sumGiamTruBaoHiem,
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCBH_LoiNhuan([FromBody] JObject data)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                ClassReportBanHang report = new ClassReportBanHang(db);
                libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
                List<BaoCaoBanHang_LoiNhuanPRC> lst = report.GetBaoCaoBanHang_LoiNhuanPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoBanHang_LoiNhuanPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoBanHang/Teamplate_BaoCaoBanHangTheoLoiNhuan.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDatHang_TongHop(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                List<BaoCaoDatHang_TongHopPRC> lst = reportDatHang.GetBaoCaoDatHang_TongHopPRC(param);
                int Rown = lst.Count();
                double SoLuongDat = lst.Sum(x => x.SoLuongDat);
                double SoLuongNhan = lst.Sum(x => x.SoLuongNhan);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiaTriDat = lst.Sum(x => x.GiaTriDat);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double tongChietKhau = lst.Sum(x => x.TongChietKhau ?? 0);
                double tongTienThue = lst.Sum(x => x.TongTienThue ?? 0);
                int lstPages = getNumber_Page(Rown, 10);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    TongChietKhau = tongChietKhau,
                    TongTienThue = tongTienThue,
                    a1 = Math.Round(SoLuongDat, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriDat, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongNhan, 3, MidpointRounding.ToEven),
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCDH_TongHop([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoDatHang_TongHopPRC> lst = reportDatHang.GetBaoCaoDatHang_TongHopPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoDatHang_TongHopPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoDatHang/Teamplate_BaoCaoDatHangTongHop.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDatHang_ChiTiet(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                List<BaoCaoDatHang_ChiTietPRC> lst = reportDatHang.GetBaoCaoDatHang_ChiTietPRC(param);
                int Rown = lst.Count();
                double SoLuongBan = lst.Sum(x => x.SoLuongDat);
                double ThanhTien = lst.Sum(x => x.TongTienHang);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double GiaTriDat = lst.Sum(x => x.GiaTriDat);
                double SoLuongNhan = lst.Sum(x => x.SoLuongNhan);
                double tongChietKhau = lst.Sum(x => x.TongChietKhau ?? 0);
                double tongTienThue = lst.Sum(x => x.TongTienThue ?? 0);
                int lstPages = getNumber_Page(Rown, 10);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    TongChietKhau = tongChietKhau,
                    TongTienThue = tongTienThue,
                    a1 = Math.Round(SoLuongBan, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriDat, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongNhan, 3, MidpointRounding.ToEven),
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCDH_ChiTiet([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoDatHang_ChiTietPRC> lst = reportDatHang.GetBaoCaoDatHang_ChiTietPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoDatHang_ChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoDatHang/Teamplate_BaoCaoDatHangChiTiet.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoDatHang_NhomHang(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                List<BaoCaoDatHang_NhomHangPRC> lst = reportDatHang.GetBaoCaoDatHang_NhomHangPRC(param);
                int Rown = lst.Count();
                double SoLuongDat = lst.Sum(x => x.SoLuongDat);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double GiaTriDat = lst.Sum(x => x.GiaTriDat);
                double SoLuongNhan = lst.Sum(x => x.SoLuongNhan);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoDatHang_NhomHangPRC> json = new JsonResultExampleTr<BaoCaoDatHang_NhomHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongDat, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriDat, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongNhan, 3, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCDH_TheoNhomHang([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportDatHang reportDatHang = new ClassReportDatHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoDatHang_NhomHangPRC> lst = reportDatHang.GetBaoCaoDatHang_NhomHangPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoDatHang_NhomHangPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoDatHang/Teamplate_BaoCaoDatHangTheoNhomHang.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhapHang_TongHop(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                    List<BaoCaoNhapHang_TongHopPRC> lst = reportNhapHang.GetBaoCaoNhapHang_TongHopPRC(param);
                    int Rown = lst.Count();
                    double SoLuong = lst.Sum(x => x.SoLuong);
                    double ThanhTien = lst.Sum(x => x.ThanhTien);
                    double GiaTriNhap = lst.Sum(x => x.GiaTriNhap);
                    double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                    int lstPages = getNumber_Page(Rown, 10);

                    return Json(new
                    {
                        res = true,
                        LstData = lst,
                        Rowcount = Rown,
                        numberPage = lstPages,
                        a1 = SoLuong,
                        a2 = ThanhTien,
                        a3 = GiamGiaHD,
                        a4 = GiaTriNhap,
                        sumTienThue = lst.Sum(x => x.TienThue),
                    });
                }
                catch (Exception ex)
                {
                    return Json(new { res = false, mes = ex.InnerException + ex.Message });
                }
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNH_TongHop([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhapHang_TongHopPRC> lst = reportNhapHang.GetBaoCaoNhapHang_TongHopPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhapHang_TongHopPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhapHang/Teamplate_BaoCaoNhapHangTongHop.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [HttpGet]
        public IHttpActionResult getList_NhomDoiTuong(int LoaiDoiTuong)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportFilter reportFilter = new ClassReportFilter(db);
                List<Report_NhomDoiTuongPRC> lst = new List<Report_NhomDoiTuongPRC>();
                lst = reportFilter.getList_NhomDoiTuong(LoaiDoiTuong);
                JsonResultExampleTr<Report_NhomDoiTuongPRC> json = new JsonResultExampleTr<Report_NhomDoiTuongPRC>
                {
                    LstData = lst,
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhapHang_ChiTiet(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                    List<BaoCaoNhapHang_ChiTietPRC> lst = new List<BaoCaoNhapHang_ChiTietPRC>();

                    lst = reportNhapHang.GetBaoCaoNhapHang_ChiTietPRC(param);
                    int Rown = lst.Count();
                    double SoLuong = lst.Sum(x => x.SoLuong);
                    double ThanhTien = lst.Sum(x => x.ThanhTien);
                    double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                    double GiaTriNhap = lst.Sum(x => x.GiaTriNhap);
                    int lstPages = getNumber_Page(Rown, 10);
                    return Json(new
                    {
                        res = true,
                        LstData = lst,
                        Rowcount = Rown,
                        numberPage = lstPages,
                        a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                        a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                        a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                        a4 = Math.Round(GiaTriNhap, 0, MidpointRounding.ToEven),
                        sumTienThue = lst.Sum(x => x.TienThue)
                    });
                }
                catch (Exception ex)
                {
                    return Json(new { res = false, mes = ex.InnerException + ex.Message });
                }
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNH_ChiTiet([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhapHang_ChiTietPRC> lst = reportNhapHang.GetBaoCaoNhapHang_ChiTietPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoNhapHang_ChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhapHang/Teamplate_BaoCaoNhapHangChiTiet.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhapHang_NhomHang(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);

                List<BaoCaoNhapHang_NhomHangPRC> lst = reportNhapHang.GetBaoCaoNhapHang_NhomHangPRC(param);
                int Rown = lst.Count();
                double SoLuongNhap = lst.Sum(x => x.SoLuongNhap);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double GiaTriNhap = lst.Sum(x => x.GiaTriNhap);
                int lstPages = getNumber_Page(Rown, 10);
                return Json(new
                {
                    res = true,
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongNhap, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriNhap, 0, MidpointRounding.ToEven),
                    sumTienThue = lst.Sum(x => x.TienThue),
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNH_TheoNhomHang([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhapHang_NhomHangPRC> lst = reportNhapHang.GetBaoCaoNhapHang_NhomHangPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhapHang_NhomHangPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhapHang/Teamplate_BaoCaoNhapHangTheoNhomHang.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhapHang_TheoNhaCungCap(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                List<BaoCaoNhapHang_TheoNhaCungCapRC> lst = reportNhapHang.GetBaoCaoNhapHang_TheoNhaCungCapRC(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuongNhap);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double GiaTriNhap = lst.Sum(x => x.GiaTriNhap);
                int lstPages = getNumber_Page(Rown, 10);
                return Json(new
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriNhap, 0, MidpointRounding.ToEven),
                    sumTienThue = lst.Sum(x => x.TienThue),
                });
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNH_TheoNhaCungCap([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhapHang_TheoNhaCungCapRC> lst = reportNhapHang.GetBaoCaoNhapHang_TheoNhaCungCapRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhapHang_TheoNhaCungCapRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhapHang/Teamplate_BaoCaoNhapHangTheoNhaCungCap.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoNhapHang_TraHangNhap(libReport.array_BaoCaoBanHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                List<BaoCaoNhapHang_TraHangNhapPRC> lst = reportNhapHang.GetBaoCaoNhapHang_TraHangNhapPRC(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                double GiamGiaHD = lst.Sum(x => x.GiamGiaHD);
                double GiaTriTra = lst.Sum(x => x.GiaTriTra);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoNhapHang_TraHangNhapPRC> json = new JsonResultExampleTr<BaoCaoNhapHang_TraHangNhapPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(GiamGiaHD, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriTra, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCNH_TraHangNhap([FromBody] JObject data)
        {
            libReport.array_BaoCaoBanHang param = data["objExcel"].ToObject<libReport.array_BaoCaoBanHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportNhapHang reportNhapHang = new ClassReportNhapHang(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoNhapHang_TraHangNhapPRC> lst = new List<BaoCaoNhapHang_TraHangNhapPRC>();
                lst = reportNhapHang.GetBaoCaoNhapHang_TraHangNhapPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoNhapHang_TraHangNhapPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoNhapHang/Teamplate_BaoCaoTraHangNhaCungCap.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }

        [AcceptVerbs("GET", "POST")]
        //Báo cáo kho hàng
        public IHttpActionResult BaoCaoKho_TonKho(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                ClassDM_HangHoa classDMHangHoa = new ClassDM_HangHoa(db);
                List<BaoCaoKho_TonKhoPRC> lst = reportKho.GetBaoCaoKho_TonKhoPRC(param);
                int Rown = lst.Count();
                double SoLuongTon = lst.Sum(x => x.TonCuoiKy);
                double SoLuongTonQuyCach = lst.Sum(x => x.TonQuyCach);
                double GiaTriTon = lst.Sum(x => x.GiaTriCuoiKy);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 0);
                JsonResultExampleTr<BaoCaoKho_TonKhoPRC> json = new JsonResultExampleTr<BaoCaoKho_TonKhoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongTon, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(SoLuongTonQuyCach, 3, MidpointRounding.ToEven),
                    a3 = Math.Round(GiaTriTon, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_TonKho_TongHop(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_TonKho_TongHopPRC> lst = reportKho.GetBaoCaoKho_TonKho_TongHopPRC(param);
                int Rown = lst.Count();
                double SoLuongTon = lst.Sum(x => x.SoLuong);
                double GiaTriTon = lst.Sum(x => x.GiaTri);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_TonKho_TongHopPRC> json = new JsonResultExampleTr<BaoCaoKho_TonKho_TongHopPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongTon, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriTon, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_TonKho([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                List<BaoCaoKho_TonKhoPRC> lst = reportKho.GetBaoCaoKho_TonKhoPRC(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoKho_TonKhoPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ID_NhomHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangHoaTonKho.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_NhapXuatTon(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_NhapXuaTonPRC> lst = reportKho.GetBaoCaoKho_NhapXuatTonPRC(param);
                int Rown = lst.Count();
                double TonDauKy = lst.Sum(x => x.TonDauKy);
                double GiaTriDauKy = lst.Sum(x => x.GiaTriDauKy);
                double SoLuongNhap = lst.Sum(x => x.SoLuongNhap);
                double GiaTriNhap = lst.Sum(x => x.GiaTriNhap);
                double SoLuongXuat = lst.Sum(x => x.SoLuongXuat);
                double GiaTriXuat = lst.Sum(x => x.GiaTriXuat);
                double TonCuoiKy = lst.Sum(x => x.TonCuoiKy);
                double TonQuyCach = lst.Sum(x => x.TonQuyCach);
                double GiaTriCuoiKy = lst.Sum(x => x.GiaTriCuoiKy);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_NhapXuaTonPRC> json = new JsonResultExampleTr<BaoCaoKho_NhapXuaTonPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(TonDauKy, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriDauKy, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongNhap, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriNhap, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongXuat, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(GiaTriXuat, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(TonCuoiKy, 3, MidpointRounding.ToEven),
                    a8 = Math.Round(TonQuyCach, 3, MidpointRounding.ToEven),
                    a9 = Math.Round(GiaTriCuoiKy, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_NhapXuatTon([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_NhapXuaTonPRC> lst = reportKho.GetBaoCaoKho_NhapXuatTonPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoKho_NhapXuaTonPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangHoaNhapXuatTon.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_NhapXuatTonChiTiet(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_NhapXuaTonChiTietPRC> lst = reportKho.GetBaoCaoKho_NhapXuatTonChiTietPRC(param);

                int Rown = lst.Count();
                double TonDauKy = lst.Sum(x => x.TonDauKy);
                double GiaTriDauKy = lst.Sum(x => x.GiaTriDauKy);
                double NhapNCC = lst.Sum(x => x.SoLuongNhap_NCC);
                double NhapKiem = lst.Sum(x => x.SoLuongNhap_Kiem);
                double NhapTra = lst.Sum(x => x.SoLuongNhap_Tra);
                double NhapChuyen = lst.Sum(x => x.SoLuongNhap_Chuyen);
                double NhapSX = lst.Sum(x => x.SoLuongNhap_SX);
                double XuatBan = lst.Sum(x => x.SoLuongXuat_Ban);
                double XuatHuy = lst.Sum(x => x.SoLuongXuat_Huy);
                double XuatNCC = lst.Sum(x => x.SoLuongXuat_NCC);
                double XuatKiem = lst.Sum(x => x.SoLuongXuat_Kiem);
                double XuatChuyen = lst.Sum(x => x.SoLuongXuat_Chuyen);
                double XuatSX = lst.Sum(x => x.SoLuongXuat_SX);
                double TonCuoiKy = lst.Sum(x => x.TonCuoiKy);
                double GiaTriCuoiKy = lst.Sum(x => x.GiaTriCuoiKy);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_NhapXuaTonChiTietPRC> json = new JsonResultExampleTr<BaoCaoKho_NhapXuaTonChiTietPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(TonDauKy, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriDauKy, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(NhapNCC, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(NhapKiem, 3, MidpointRounding.ToEven),
                    a5 = Math.Round(NhapTra, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(NhapChuyen, 3, MidpointRounding.ToEven),
                    a7 = Math.Round(NhapSX, 3, MidpointRounding.ToEven),
                    a8 = Math.Round(XuatBan, 3, MidpointRounding.ToEven),
                    a9 = Math.Round(XuatHuy, 3, MidpointRounding.ToEven),
                    a10 = Math.Round(XuatNCC, 3, MidpointRounding.ToEven),
                    a11 = Math.Round(XuatKiem, 3, MidpointRounding.ToEven),
                    a12 = Math.Round(XuatChuyen, 3, MidpointRounding.ToEven),
                    a13 = Math.Round(XuatSX, 3, MidpointRounding.ToEven),
                    a14 = Math.Round(TonCuoiKy, 3, MidpointRounding.ToEven),
                    a15 = Math.Round(GiaTriCuoiKy, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_NhapXuatTonChiTiet([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_NhapXuaTonChiTietPRC> lst = reportKho.GetBaoCaoKho_NhapXuatTonChiTietPRC(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoKho_NhapXuaTonChiTietPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                excel.Columns.Remove("SoLuongNhap_SX");
                excel.Columns.Remove("SoLuongXuat_SX");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangHoaNhapXuatTonChiTiet.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, param.columnsHide, lstDataCell);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_XuatChuyenHang(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangPRC(true, param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_XuatChuyenHangChiTiet(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangChiTietPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangChiTietPRC(true, param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.GiaTri);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangChiTietPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangChiTietPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_XuatChuyenHang([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangPRC(true, param);
                List<BaoCaoKho_XuatChuyenHangChiTietPRC> lstCT = reportKho.GetBaoCaoKho_XuatChuyenHangChiTietPRC(true, param);
                DataTable excel = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangHoaXuatChuyenHang.xlsx");

                DataTable excelCT = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangChiTietPRC>(lstCT);
                excelCT.Columns.Remove("TenHangHoa");
                excelCT.Columns.Remove("ThuocTinh_GiaTri");
                excelCT.Columns.Remove("MaChiNhanhChuyen");
                excelCT.Columns.Remove("MaChiNhanhNhan");

                List<System.Data.DataTable> lstDataTable = new List<System.Data.DataTable>
                {
                    excel,excelCT
                };
                List<ClassExcel_CellData> lstCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                List<Excel_ParamExport> lstPr = new List<Excel_ParamExport> {
                     new Excel_ParamExport
                     {
                        SheetIndex=0,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHide ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     },
                        new Excel_ParamExport
                     {
                        SheetIndex=1,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHideCT ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     }
                };
                HttpResponseMessage response = classAposeCell.ExportData_ToMultipleSheet(fileTeamplate, lstDataTable, lstPr);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_NhapChuyenHang(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangPRC(false, param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_NhapChuyenHangChiTiet(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangChiTietPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangChiTietPRC(false, param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.GiaTri);
                double ThanhTien = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangChiTietPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangChiTietPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(ThanhTien, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_NhapChuyenHang([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_XuatChuyenHangPRC(false, param);
                List<BaoCaoKho_XuatChuyenHangChiTietPRC> lstCT = reportKho.GetBaoCaoKho_XuatChuyenHangChiTietPRC(false, param);
                DataTable excel = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");

                DataTable excelCT = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangChiTietPRC>(lstCT);
                excelCT.Columns.Remove("TenHangHoa");
                excelCT.Columns.Remove("ThuocTinh_GiaTri");
                excelCT.Columns.Remove("MaChiNhanhChuyen");
                excelCT.Columns.Remove("MaChiNhanhNhan");

                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangHoaNhapChuyenHang.xlsx");

                List<System.Data.DataTable> lstDataTable = new List<System.Data.DataTable>
                {
                    excel,excelCT
                };
                List<ClassExcel_CellData> lstCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                List<Excel_ParamExport> lstPr = new List<Excel_ParamExport> {
                     new Excel_ParamExport
                     {
                        SheetIndex=0,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHide ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     },
                        new Excel_ParamExport
                     {
                        SheetIndex=1,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHideCT ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     }
                };
                HttpResponseMessage response = classAposeCell.ExportData_ToMultipleSheet(fileTeamplate, lstDataTable, lstPr);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_TongHopHangNhapKho(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_TongHopHangNhapXuatKho(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_TongHopHangPTHong(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_TongHopHangNhapPTHong(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_ChiTietHangNhapKho(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_ChiTietHangNhapKhoPRC> lst = reportKho.GetBaoCaoKho_ChiTietHangNhapXuatKho(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC> json = new JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_ChiTietHangNhapKhoPTHong(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_ChiTietHangNhapKhoPRC> lst = reportKho.GetBaoCaoKho_ChiTietHangNhapKhoPTHong(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC> json = new JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_TongHopHangNhapKho([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_TongHopHangNhapXuatKho(param);
                List<BaoCaoKho_ChiTietHangNhapKhoPRC> lstCT = reportKho.GetBaoCaoKho_ChiTietHangNhapXuatKho(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");

                DataTable excelCT = classOffice.ToDataTable<BaoCaoKho_ChiTietHangNhapKhoPRC>(lstCT);
                excelCT.Columns.Remove("TenHangHoa");
                excelCT.Columns.Remove("ThuocTinh_GiaTri");
                excelCT.Columns.Remove("BienSo");

                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoTongHopHangNhapKho.xlsx");

                List<System.Data.DataTable> lstDataTable = new List<System.Data.DataTable>
                {
                    excel,excelCT
                };
                List<ClassExcel_CellData> lstCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                List<Excel_ParamExport> lstPr = new List<Excel_ParamExport> {
                     new Excel_ParamExport
                     {
                        SheetIndex=0,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHide ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     },
                        new Excel_ParamExport
                     {
                        SheetIndex=1,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHideCT ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     }
                };
                HttpResponseMessage response = classAposeCell.ExportData_ToMultipleSheet(fileTeamplate, lstDataTable, lstPr);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_TongHopHangXuatKho(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_TongHopHangNhapXuatKho(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatChuyenHangPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_ChiTietHangXuatKho(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_ChiTietHangNhapKhoPRC> lst = reportKho.GetBaoCaoKho_ChiTietHangNhapXuatKho(param);
                int Rown = lst.Count();
                double SoLuong = lst.Sum(x => x.SoLuong);
                double GiaTri = lst.Sum(x => x.ThanhTien);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC> json = new JsonResultExampleTr<BaoCaoKho_ChiTietHangNhapKhoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuong, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTri, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_TongHopHangXuatKho([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_XuatChuyenHangPRC> lst = reportKho.GetBaoCaoKho_TongHopHangNhapXuatKho(param);
                List<BaoCaoKho_ChiTietHangNhapKhoPRC> lstCT = reportKho.GetBaoCaoKho_ChiTietHangNhapXuatKho(param);

                DataTable excel = classOffice.ToDataTable<BaoCaoKho_XuatChuyenHangPRC>(lst);
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");

                DataTable excelCT = classOffice.ToDataTable<BaoCaoKho_ChiTietHangNhapKhoPRC>(lstCT);
                excelCT.Columns.Remove("TenHangHoa");
                excelCT.Columns.Remove("ThuocTinh_GiaTri");

                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoTongHopHangXuatKho.xlsx");

                List<System.Data.DataTable> lstDataTable = new List<System.Data.DataTable>
                {
                    excel,excelCT
                };
                List<ClassExcel_CellData> lstCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                List<Excel_ParamExport> lstPr = new List<Excel_ParamExport> {
                     new Excel_ParamExport
                     {
                        SheetIndex=0,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHide ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     },
                        new Excel_ParamExport
                     {
                        SheetIndex=1,
                        StartRow =  4,
                        EndRow = 28,
                        ColumnsHide = param.columnsHideCT ,
                        HasRowSum_AtLastIndex = true,
                        CellData = lstCell
                     }
                };
                HttpResponseMessage response = classAposeCell.ExportData_ToMultipleSheet(fileTeamplate, lstDataTable, lstPr);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCK_XuatDinhLuongDichVu([FromBody] JObject data)
        {
            array_BaoCaoKhoHang param = data["objExcel"].ToObject<array_BaoCaoKhoHang>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();

                List<BaoCaoKho_XuatDichVuDinhLuongPRC> lst = reportKho.GetBaoCaoKho_XuatDichVuDinhLuong(param);
                DataTable excel = classOffice.ToDataTable<BaoCaoKho_XuatDichVuDinhLuongPRC>(lst);
                var nganhnghe = CookieStore.GetCookieAes("shop").ToUpper();
                var columnRemove = string.Empty;
                if (nganhnghe != "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                {
                    columnRemove = "3_4_";
                }
                if (!string.IsNullOrEmpty(param.columnsHide))
                {
                    columnRemove += param.columnsHide;
                }
                excel.Columns.Remove("TenHangHoa");
                excel.Columns.Remove("ThuocTinh_GiaTri");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoKho/Teamplate_BaoCaoHangXuatKhoTheoDichVuDinhLuong.xlsx");
                var lstDataCell = classAposeCell.GetData_ForDefaultCell(param.TenChiNhanh, param.TodayBC);
                HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 28, true, columnRemove);
                return response;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoKho_XuatDichVuDinhLuong(array_BaoCaoKhoHang param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportKho reportKho = new ClassReportKho(db);
                List<BaoCaoKho_XuatDichVuDinhLuongPRC> lst = reportKho.GetBaoCaoKho_XuatDichVuDinhLuong(param);
                int Rown = lst.Count();
                List<BaoCaoKho_XuatDichVuDinhLuongPRC> lst_gr = lst.GroupBy(x => new { x.MaHoaDon, x.MaDichVu, x.SoLuongDichVu }).Select(t => new BaoCaoKho_XuatDichVuDinhLuongPRC
                {
                    SoLuongDichVu = t.FirstOrDefault().SoLuongDichVu,
                    GiaTriDichVu = t.FirstOrDefault().GiaTriDichVu
                }).ToList();
                double SoLuongDichVu = lst_gr.Sum(x => x.SoLuongDichVu);
                double GiaTriDichVu = lst_gr.Sum(x => x.GiaTriDichVu);
                double SoLuongBanDau = lst.Sum(x => x.SoLuongDinhLuongBanDau);
                double GiaTriGiaTriBanDau = lst.Sum(x => x.GiaTriDinhLuongBanDau);
                double SoLuongThucTe = lst.Sum(x => x.SoLuongThucTe);
                double GiaTriGiaTriThucTe = lst.Sum(x => x.GiaTriThucTe);
                double SoLuongChenhLech = lst.Sum(x => x.SoLuongChenhLech);
                double GiaTriChenhLech = lst.Sum(x => x.GiaTriChenhLech);
                int lstPages = getNumber_Page(Rown, param.PageSize ?? 10);
                JsonResultExampleTr<BaoCaoKho_XuatDichVuDinhLuongPRC> json = new JsonResultExampleTr<BaoCaoKho_XuatDichVuDinhLuongPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(SoLuongDichVu, 3, MidpointRounding.ToEven),
                    a2 = Math.Round(GiaTriDichVu, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(SoLuongBanDau, 3, MidpointRounding.ToEven),
                    a4 = Math.Round(GiaTriGiaTriBanDau, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(SoLuongThucTe, 3, MidpointRounding.ToEven),
                    a6 = Math.Round(GiaTriGiaTriThucTe, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(SoLuongChenhLech, 3, MidpointRounding.ToEven),
                    a8 = Math.Round(GiaTriChenhLech, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_CongNo(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_CongNoPRC> lst = new List<BaoCaoTaiChinh_CongNoPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }

                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";

                lst = reportTaiChinh.GetBaoCaoTaiChinh_CongNo(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, array_BaoCaoTaiChinh.ID_DonVi, LaDT_search, ID_NhomDoiTuong_search,
                    ID_NhomDoiTuong_SP);
                int Rown = lst.Count();
                double PhaiTraDauKy = lst.Sum(x => x.PhaiTraDauKy);
                double PhaiThuDauKy = lst.Sum(x => x.PhaiThuDauKy);
                double TongTienChi = lst.Sum(x => x.TongTienChi);
                double TongTienThu = lst.Sum(x => x.TongTienThu);
                double PhaiTraCuoiKy = lst.Sum(x => x.PhaiTraCuoiKy);
                double PhaiThuCuoiKy = lst.Sum(x => x.PhaiThuCuoiKy);
                int lstPages = getNumber_Page(Rown, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_CongNoPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_CongNoPRC>
                {
                    LstData = lst,
                    Rowcount = Rown,
                    numberPage = lstPages,
                    a1 = Math.Round(PhaiThuDauKy, 0, MidpointRounding.ToEven),
                    a2 = Math.Round(PhaiTraDauKy, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(TongTienChi, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(TongTienThu, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(PhaiThuCuoiKy, 0, MidpointRounding.ToEven),
                    a6 = Math.Round(PhaiTraCuoiKy, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_CongNo_v2([FromBody] JObject objIn)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    Guid IdChiNhanh = Guid.Empty;
                    if (objIn["IdChiNhanhs"] != null)
                        IdChiNhanh = objIn["IdChiNhanhs"].ToObject<Guid>();
                    string textSearch = "";
                    if (objIn["TextSearch"] != null)
                        textSearch = objIn["TextSearch"].ToObject<string>();
                    DateTime timeStart = DateTime.Now;
                    if (objIn["TimeStart"] != null)
                    {
                        timeStart = objIn["TimeStart"].ToObject<DateTime>();
                    }
                    DateTime timeEnd = DateTime.Now;
                    if (objIn["TimeEnd"] != null)
                    {
                        timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                    }
                    string LoaiKH = "";
                    if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                    {
                        LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                    }
                    string IdNhomDoiTuong = "";
                    string IdNhomKhachHang = "";
                    if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                    {
                        IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                    }
                    string IdNhomNhaCungCap = "";
                    if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                    {
                        IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                    }
                    if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                    {
                        IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                    }
                    else if (IdNhomKhachHang != "")
                    {
                        IdNhomDoiTuong = IdNhomKhachHang;
                    }
                    else
                    {
                        IdNhomDoiTuong = IdNhomNhaCungCap;
                    }
                    int TrangThaiCongNo = 0;
                    if (objIn["TrangThaiCongNo"] != null)
                        TrangThaiCongNo = objIn["TrangThaiCongNo"].ToObject<int>();
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_CongNoPRC> lstResult = reportTaiChinh.GetBaoCaoTaiChinh_CongNo_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong);
                    if (TrangThaiCongNo == 1)
                    {
                        lstResult = lstResult.Where(p => p.TongTienChi != 0 || p.TongTienThu != 0).ToList();
                    }
                    else if (TrangThaiCongNo == 2)
                    {
                        lstResult = lstResult.Where(p => p.PhaiTraCuoiKy != 0 || p.PhaiThuCuoiKy != 0).ToList();
                    }
                    return ActionTrueData(new
                    {
                        data = lstResult,
                        sPhaiTraDauKy = lstResult.Sum(x => x.PhaiTraDauKy),
                        sPhaiThuDauKy = lstResult.Sum(x => x.PhaiThuDauKy),
                        sTongTienChi = lstResult.Sum(x => x.TongTienChi),
                        sTongTienThu = lstResult.Sum(x => x.TongTienThu),
                        sPhaiTraCuoiKy = lstResult.Sum(x => x.PhaiTraCuoiKy),
                        sPhaiThuCuoiKy = lstResult.Sum(x => x.PhaiThuCuoiKy),
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_BaoCaoTaiChinh_CongNo_v2([FromBody] JObject objIn)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    Guid IdChiNhanh = Guid.Empty;
                    if (objIn["IdChiNhanhs"] != null)
                        IdChiNhanh = objIn["IdChiNhanhs"].ToObject<Guid>();
                    string textSearch = "";
                    if (objIn["TextSearch"] != null)
                        textSearch = objIn["TextSearch"].ToObject<string>();
                    DateTime timeStart = DateTime.Now;
                    if (objIn["TimeStart"] != null)
                    {
                        timeStart = objIn["TimeStart"].ToObject<DateTime>();
                    }
                    DateTime timeEnd = DateTime.Now;
                    if (objIn["TimeEnd"] != null)
                    {
                        timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                    }
                    string LoaiKH = "";
                    if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                    {
                        LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                    }
                    string IdNhomDoiTuong = "";
                    string IdNhomKhachHang = "";
                    if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                    {
                        IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                    }
                    string IdNhomNhaCungCap = "";
                    if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                    {
                        IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                    }
                    if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                    {
                        IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                    }
                    else if (IdNhomKhachHang != "")
                    {
                        IdNhomDoiTuong = IdNhomKhachHang;
                    }
                    else
                    {
                        IdNhomDoiTuong = IdNhomNhaCungCap;
                    }
                    List<int> lstColHide = new List<int>();
                    List<int> lstColHideTemp = new List<int>();
                    if (objIn["ColHide"] != null)
                        lstColHideTemp = objIn["ColHide"].ToObject<List<int>>();
                    foreach (int item in lstColHideTemp)
                    {
                        if (item < 3)
                        {
                            lstColHide.Add(item);
                        }
                        else if (item == 3)
                        {
                            lstColHide.Add(3);
                            lstColHide.Add(4);
                        }
                        else if (item == 4)
                        {
                            lstColHide.Add(5);
                            lstColHide.Add(6);
                        }
                        else if (item == 5)
                        {
                            lstColHide.Add(7);
                            lstColHide.Add(8);
                        }
                    }


                    string BaoCaoThoiGian = "";
                    if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                        BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                    string BaoCaoChiNhanh = "";
                    if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                        BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                    int TrangThaiCongNo = 0;
                    if (objIn["TrangThaiCongNo"] != null)
                        TrangThaiCongNo = objIn["TrangThaiCongNo"].ToObject<int>();
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_CongNoPRC> lstResult = reportTaiChinh.GetBaoCaoTaiChinh_CongNo_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong);
                    if (TrangThaiCongNo == 1)
                    {
                        lstResult = lstResult.Where(p => p.TongTienChi != 0 || p.TongTienThu != 0).ToList();
                    }
                    else if (TrangThaiCongNo == 2)
                    {
                        lstResult = lstResult.Where(p => p.PhaiTraCuoiKy != 0 || p.PhaiThuCuoiKy != 0).ToList();
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_CongNoPRC>(lstResult);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoTongHopCongNo.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_CongNo([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_CongNoPRC> lst = new List<BaoCaoTaiChinh_CongNoPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                string a = array_BaoCaoTaiChinh.ID_NhomNhaCungCap;
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang.ToString() != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }

                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";

                lst = reportTaiChinh.GetBaoCaoTaiChinh_CongNo(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, array_BaoCaoTaiChinh.ID_DonVi, LaDT_search, ID_NhomDoiTuong_search,
                    ID_NhomDoiTuong_SP);
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_CongNoPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoTongHopCongNo.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoTongHopCongNo.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 5, 29, 24, true, array_BaoCaoTaiChinh.columnsHide, array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoTongHopCongNo.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_ThuChi(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }

                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";

                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs,
                    LaDT_search, ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search);
                int Rows = lst.Count();
                double ThuChi = (double?)lst.Sum(x => x.ThuChi) ?? 0;
                //List<ListLHPages> lstPages = getAllPage<BaoCaoTaiChinh_ThuChiPRC>(lst, 10);
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_ThuChiPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_ThuChiPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages,
                    a1 = Math.Round(ThuChi, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_ThuChi_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    timeStart = objIn["TimeStart"].ToObject<DateTime>();
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh);
                    return ActionTrueData(new
                    {
                        data = lst,
                        sTongThuChi = lst.Sum(p => p.ThuChi)
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_NhatKyThuTien_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    timeStart = objIn["TimeStart"].ToObject<DateTime>();
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh);
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_ThuChiPRC>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoNhatKyThuTien.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_NhatKyChiTien_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    timeStart = objIn["TimeStart"].ToObject<DateTime>();
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh);
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_ThuChiPRC>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoNhatKyChiTien.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }


        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_NhatKyThuTien([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != "")
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != "")
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != "")
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs,
                    LaDT_search, ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search);
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_ThuChiPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoNhatKyThuTien.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoNhatKyThuTien.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoNhatKyThuTien.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_NhatKyChiTien([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_ThuChiPRC> lst = new List<BaoCaoTaiChinh_ThuChiPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != "")
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != "")
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_ThuChi(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs,
                    LaDT_search, ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search);
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_ThuChiPRC>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoNhatKyChiTien.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoNhatKyChiTien.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoNhatKyChiTien.xlsx");
                return fileSave;
            }
        }
        public IHttpActionResult BaoCaoTaiChinh_SoQuyTheoChiNhanh(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC> lst = new List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuyTheoChiNhanh(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search, ID_NhomDoiTuong_search,
                    ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search);
                int Rows = lst.Count();
                double TonTienMat = lst.Sum(x => x.TonTienMat);
                double TonTienGui = lst.Sum(x => x.TonTienGui);
                double TongThuChi = lst.Sum(x => x.TongThuChi);
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages,
                    a1 = Math.Round(TonTienMat, 0, MidpointRounding.ToEven),
                    a2 = Math.Round(TonTienGui, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(TongThuChi, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_SoQuyTheoChiNhanh_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";

                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    string tempDate = objIn["TimeEnd"].ToObject<string>();
                    timeEnd = DateTime.ParseExact(tempDate, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    //timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }

                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC> lst = new List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuyTheoChiNhanh_v2(timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh);
                    double TonTienMat = lst.Sum(x => x.TonTienMat);
                    double TonTienGui = lst.Sum(x => x.TonTienGui);
                    double TongThuChi = lst.Sum(x => x.TongThuChi);
                    return ActionTrueData(new
                    {
                        data = lst,
                        STongTienMat = TonTienMat,
                        STongTienGui = TonTienGui,
                        STongThuChi = TongThuChi
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_SoQuy(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);

                double TonDauKy = 0;
                if (lst.Count() != 0)
                {
                    TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                }
                double TongThu = lst.Sum(x => x.TienThu);
                double TongChi = lst.Sum(x => x.TienChi);
                double TongThuTienMat = lst.Sum(x => x.ThuTienMat);
                double TongChiTienMat = lst.Sum(x => x.ChiTienMat);
                double TongThuTienGui = lst.Sum(x => x.ThuTienGui);
                double TongChiTienGui = lst.Sum(x => x.ChiTienGui);
                double TonTrongKy = TongThu - TongChi;
                double TonTrongKyTienGui = TongThuTienGui - TongChiTienGui;
                double TonTrongKyTienMat = TongThuTienMat - TongChiTienMat;
                double TonCuoiKy = TonDauKy + TonTrongKy;
                double TonCuoiKyTienMat = TonDauKy + TonTrongKyTienMat;
                double TonCuoiKyTienGui = TonDauKy + TonTrongKyTienGui;

                //List<ListLHPages> lstPages = getAllPage<BaoCaoTaiChinh_SoQuyPRC>(lst, 10);
                if (lst.FirstOrDefault().MaPhieuThu.ToString() == "TRINH0001")
                {
                    lst.RemoveAll(r => r.MaPhieuThu == "TRINH0001");
                }
                int Rows = lst.Count();
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_SoQuyPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_SoQuyPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages,
                    a1 = Math.Round(TonDauKy, 0, MidpointRounding.ToEven),
                    a2 = Math.Round(TongThu, 0, MidpointRounding.ToEven),
                    a3 = Math.Round(TongChi, 0, MidpointRounding.ToEven),
                    a4 = Math.Round(TongThuTienMat, 0, MidpointRounding.ToEven),
                    a5 = Math.Round(TongChiTienMat, 0, MidpointRounding.ToEven),
                    a6 = Math.Round(TongThuTienGui, 0, MidpointRounding.ToEven),
                    a7 = Math.Round(TongChiTienGui, 0, MidpointRounding.ToEven),
                    a8 = Math.Round(TonTrongKy, 0, MidpointRounding.ToEven),
                    a9 = Math.Round(TonCuoiKy, 0, MidpointRounding.ToEven),
                    a10 = Math.Round(TonTrongKyTienMat, 0, MidpointRounding.ToEven),
                    a11 = Math.Round(TonCuoiKyTienMat, 0, MidpointRounding.ToEven),
                    a12 = Math.Round(TonTrongKyTienGui, 0, MidpointRounding.ToEven),
                    a13 = Math.Round(TonCuoiKyTienGui, 0, MidpointRounding.ToEven),
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_SoQuy_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    if (objIn["TimeStart"].ToObject<string>() != "")
                    {
                        timeStart = objIn["TimeStart"].ToObject<DateTime>();
                    }
                    else
                    {
                        timeStart = DateTime.Parse("2010-01-01");
                    }
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    if (objIn["TimeEnd"].ToObject<string>() != "")
                        timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                    else
                    {
                        timeEnd = DateTime.Now.AddMonths(1);
                    }
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    double TonDauKy = 0;
                    if (lst.Count() != 0)
                    {
                        TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                    }
                    double TongThu = 0;
                    double TongChi = 0;
                    if (intLoaiTien == 1)
                    {
                        TongThu = lst.Sum(x => x.ThuTienMat);
                        TongChi = lst.Sum(x => x.ChiTienMat);
                    }
                    else if (intLoaiTien == 2)
                    {
                        TongThu = lst.Sum(x => x.ThuTienGui);
                        TongChi = lst.Sum(x => x.ChiTienGui);
                    }
                    else
                    {
                        TongThu = lst.Sum(x => x.TienThu);
                        TongChi = lst.Sum(x => x.TienChi);
                    }

                    double PhatSinhTrongKy = TongThu - TongChi;
                    double SoDuCuoiKy = TonDauKy + PhatSinhTrongKy;

                    //List<ListLHPages> lstPages = getAllPage<BaoCaoTaiChinh_SoQuyPRC>(lst, 10);
                    if (lst.FirstOrDefault().MaPhieuThu.ToString() == "TRINH0001")
                    {
                        lst.RemoveAll(r => r.MaPhieuThu == "TRINH0001");
                    }
                    return ActionTrueData(new
                    {
                        data = lst,
                        TongThu = TongThu,
                        TongChi = TongChi,
                        SoDuDauKy = TonDauKy,
                        PhatSinhTrongKy = PhatSinhTrongKy,
                        SoDuCuoiKy = SoDuCuoiKy
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoThang(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_PTTCTheoThangPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoThangPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                switch (array_BaoCaoTaiChinh.LoaiTien)
                {
                    case 1:
                        LoaiTien_Search = "%1%";
                        break;
                    case 2:
                        LoaiTien_Search = "%2%";
                        break;
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }

                string LaDT_search = "%%";
                switch (array_BaoCaoTaiChinh.LoaiDoiTuong)
                {
                    case 0:
                        LaDT_search = "%2%";
                        break;
                    case 1:
                        LaDT_search = "%1%";
                        break;
                }
                string HachToanKD_Search = "%%";
                switch (array_BaoCaoTaiChinh.HachToanKD)
                {
                    case 1:
                        HachToanKD_Search = "%1%";
                        break;
                    case 2:
                        HachToanKD_Search = "%0%";
                        break;
                }
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoThang(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int Rows = lst.Count();
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoThangPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoThangPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoThang_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_PTTCTheoThangPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoThangPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoThang_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    return ActionTrueData(new
                    {
                        data = lst
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_PhanTichThuChiTheoThang_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_PTTCTheoThangPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoThangPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoThang_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    int TongThu = 0, TongChi = lst.Count() + 3;
                    int i = 0;
                    foreach (var item in lst)
                    {
                        i = i + 1;
                        if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                        {
                            TongThu = i + 3;
                            break;
                        }
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoThangPRC>(lst);
                    excel.Columns.Remove("ID_KhoanThuChi");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoThang.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);

                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, null, lstDataCell);
                    return response;
                    //classOffice.listToOfficeExcel_PhanTichThuChi(fileTeamplate, fileSave, excel, 4, 28, 24, true, null, "Năm " + Year, BaoCaoChiNhanh, TongThu, TongChi);
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_PhanTichThuChiTheoThang([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_PTTCTheoThangPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoThangPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoThang(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int TongThu = 0, TongChi = lst.Count() + 3;
                int i = 0;
                foreach (var item in lst)
                {
                    i = i + 1;
                    if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                    {
                        TongThu = i + 3;
                        break;
                    }
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoThangPRC>(lst);
                excel.Columns.Remove("ID_KhoanThuChi");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoThang.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoThang.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_PhanTichThuChi(fileTeamplate, fileSave, excel, 4, 28, 24, true, null, array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, TongThu, TongChi);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoThang.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_PhanTichThuChiTheoQuy_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_PTTCTheoQuyPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoQuyPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoQuy_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    int TongThu = 0, TongChi = lst.Count() + 3;
                    int i = 0;
                    foreach (var item in lst)
                    {
                        i = i + 1;
                        if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                        {
                            TongThu = i + 3;
                            break;
                        }
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoQuyPRC>(lst);
                    excel.Columns.Remove("ID_KhoanThuChi");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoQuy.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, null, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_PhanTichThuChiTheoQuy([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_PTTCTheoQuyPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int TongThu = 0, TongChi = lst.Count() + 3;
                int i = 0;
                foreach (var item in lst)
                {
                    i = i + 1;
                    if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                    {
                        TongThu = i + 3;
                        break;
                    }
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoQuyPRC>(lst);
                excel.Columns.Remove("ID_KhoanThuChi");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoQuy.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoQuy.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_PhanTichThuChi(fileTeamplate, fileSave, excel, 4, 28, 24, true, null, array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, TongThu, TongChi);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoQuy.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_PhanTichThuChiTheoNam_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_PTTCTheoNamPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoNamPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoNam_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    int TongThu = 0, TongChi = lst.Count() + 3;
                    int i = 0;
                    foreach (var item in lst)
                    {
                        i = i + 1;
                        if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                        {
                            TongThu = i + 3;
                            break;
                        }
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoNamPRC>(lst);
                    excel.Columns.Remove("ID_KhoanThuChi");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoNam.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, null, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_PhanTichThuChiTheoNam([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_PTTCTheoNamPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoNamPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoNam(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int TongThu = 0, TongChi = lst.Count() + 3;
                int i = 0;
                foreach (var item in lst)
                {
                    i = i + 1;
                    if (item.ID_KhoanThuChi.ToString() == "00000010-0000-0000-0000-000000000010")
                    {
                        TongThu = i + 3;
                        break;
                    }
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_PTTCTheoNamPRC>(lst);
                excel.Columns.Remove("ID_KhoanThuChi");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_PhanTichThuChiTheoNam.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoNam.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_PhanTichThuChi(fileTeamplate, fileSave, excel, 4, 28, 24, true, null, array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, TongThu, TongChi);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/PhanTichThuChiTheoNam.xlsx");
                return fileSave;
            }
        }
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoQuy(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_PTTCTheoQuyPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int Rows = lst.Count();
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoQuyPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoQuyPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoQuy_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_PTTCTheoQuyPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoQuyPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoQuy_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    return ActionTrueData(new
                    {
                        data = lst
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoNam(array_BaoCaoTaiChinh array_BaoCaoTaiChinh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                List<BaoCaoTaiChinh_PTTCTheoNamPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoNamPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoNam(maKH_search, maKH_TV, array_BaoCaoTaiChinh.year, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                int Rows = lst.Count();
                int lstPages = getNumber_Page(Rows, 10);
                JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoNamPRC> json = new JsonResultExampleTr<BaoCaoTaiChinh_PTTCTheoNamPRC>
                {
                    LstData = lst,
                    Rowcount = Rows,
                    numberPage = lstPages
                };
                return Json(json);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult BaoCaoTaiChinh_PhanTichThuChiTheoNam_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    List<BaoCaoTaiChinh_PTTCTheoNamPRC> lst = new List<BaoCaoTaiChinh_PTTCTheoNamPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_PhanTichThuChiTheoNam_v2(Year, IdChiNhanh, LoaiKH,
                        IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    return ActionTrueData(new
                    {
                        data = lst
                    });
                }
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_SoQuyTienMat_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    timeStart = objIn["TimeStart"].ToObject<DateTime>();
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string strColHide = "";
                if (lstColHide.Count > 0)
                {
                    strColHide = string.Join("_", lstColHide);
                }
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    double TonDauKy = 0;
                    if (lst.Count() != 0)
                    {
                        TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                    excel.Columns.Remove("TonDauKy");
                    excel.Columns.Remove("TienMat");
                    excel.Columns.Remove("TienGui");
                    excel.Columns.Remove("TienThu");
                    excel.Columns.Remove("TienChi");
                    excel.Columns.Remove("ThuTienGui");
                    excel.Columns.Remove("ChiTienGui");
                    excel.Columns.Remove("SoTaiKhoan");
                    excel.Columns.Remove("TonLuyKeTienGui");
                    excel.Columns.Remove("TonLuyKe");
                    excel.Columns.Remove("NganHang");
                    excel.Columns.Remove("IDDonVi");
                    excel.Columns.Remove("ID_HoaDon");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyTienMat.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    lstDataCell.Add(new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 7, CellValue = TonDauKy.ToString(), IsNumber = true });
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_SoQuyTienMat([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                double TonDauKy = 0;
                if (lst.Count() != 0)
                {
                    TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                excel.Columns.Remove("TonDauKy");
                excel.Columns.Remove("TienMat");
                excel.Columns.Remove("TienGui");
                excel.Columns.Remove("TienThu");
                excel.Columns.Remove("TienChi");
                excel.Columns.Remove("ThuTienGui");
                excel.Columns.Remove("ChiTienGui");
                excel.Columns.Remove("SoTaiKhoan");
                excel.Columns.Remove("TonLuyKeTienGui");
                excel.Columns.Remove("TonLuyKe");
                excel.Columns.Remove("NganHang");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyTienMat.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyTienMat.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_StypeSQ(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, 7, TonDauKy);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyTienMat.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_SoQuyNganHang_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    timeStart = objIn["TimeStart"].ToObject<DateTime>();
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string strColHide = "";
                if (lstColHide.Count > 0)
                {
                    strColHide = string.Join("_", lstColHide);
                }
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    double TonDauKy = 0;
                    if (lst.Count() != 0)
                    {
                        TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                    excel.Columns.Remove("TonDauKy");
                    excel.Columns.Remove("TienMat");
                    excel.Columns.Remove("TienGui");
                    excel.Columns.Remove("TienThu");
                    excel.Columns.Remove("TienChi");
                    excel.Columns.Remove("ThuTienMat");
                    excel.Columns.Remove("ChiTienMat");
                    excel.Columns.Remove("TonLuyKeTienMat");
                    excel.Columns.Remove("TonLuyKe");
                    excel.Columns.Remove("IDDonVi");
                    excel.Columns.Remove("ID_HoaDon");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyNganHang.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    lstDataCell.Add(new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 9, CellValue = TonDauKy.ToString(), IsNumber = true });
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, strColHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_SoQuyNganHang([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                double TonDauKy = 0;
                if (lst.Count() != 0)
                {
                    TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                excel.Columns.Remove("TonDauKy");
                excel.Columns.Remove("TienMat");
                excel.Columns.Remove("TienGui");
                excel.Columns.Remove("TienThu");
                excel.Columns.Remove("TienChi");
                excel.Columns.Remove("ThuTienMat");
                excel.Columns.Remove("ChiTienMat");
                excel.Columns.Remove("TonLuyKeTienMat");
                excel.Columns.Remove("TonLuyKe");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyNganHang.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyNganHang.xlsx");
                //classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, true, columnsHide.Trim(), TodayBC, TenChiNhanh);
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_StypeSQ(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, 9, TonDauKy);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyNganHang.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_SoQuyTongQuy_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";
                if (objIn["TextSearch"] != null)
                    textSearch = objIn["TextSearch"].ToObject<string>();
                DateTime timeStart = DateTime.Now;
                if (objIn["TimeStart"] != null)
                {
                    if (objIn["TimeStart"].ToObject<string>() != "")
                    {
                        timeStart = objIn["TimeStart"].ToObject<DateTime>();
                    }
                    else
                    {
                        timeStart = DateTime.Parse("2010-01-01");
                    }
                }
                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    if (objIn["TimeEnd"].ToObject<string>() != "")
                        timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                    else
                    {
                        timeEnd = DateTime.Now.AddMonths(1);
                    }
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                string LoaiTien = "%%";
                int intLoaiTien = 0;
                if (objIn["LoaiTien"] != null && objIn["LoaiTien"].ToObject<string>() != "")
                {
                    intLoaiTien = int.Parse(objIn["LoaiTien"].ToObject<string>());
                    if (intLoaiTien == 1)
                    {
                        LoaiTien = "%1%";
                    }
                    else if (intLoaiTien == 2)
                    {
                        LoaiTien = "%2%";
                    }
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string strColHide = "";
                if (lstColHide.Count > 0)
                {
                    strColHide = string.Join("_", lstColHide);
                }
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy_v2(textSearch, timeStart, timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh, LoaiTien);
                    double TonDauKy = 0;
                    if (lst.Count() != 0)
                    {
                        TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                    }
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                    excel.Columns.Remove("TonDauKy");
                    excel.Columns.Remove("TienMat");
                    excel.Columns.Remove("TienGui");
                    excel.Columns.Remove("ThuTienMat");
                    excel.Columns.Remove("ChiTienMat");
                    excel.Columns.Remove("ThuTienGui");
                    excel.Columns.Remove("ChiTienGui");
                    excel.Columns.Remove("TonLuyKeTienGui");
                    excel.Columns.Remove("TonLuyKeTienMat");
                    excel.Columns.Remove("IDDonVi");
                    excel.Columns.Remove("ID_HoaDon");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyTongQuy.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    lstDataCell.Add(new ClassExcel_CellData { RowIndex = 2, ColumnIndex = 9, CellValue = TonDauKy.ToString(), IsNumber = true });
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, strColHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_SoQuyTongQuy([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_SoQuyPRC> lst = new List<BaoCaoTaiChinh_SoQuyPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                string LoaiTien_Search = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                if (array_BaoCaoTaiChinh.LoaiTien == 1)
                    LoaiTien_Search = "%1%";
                if (array_BaoCaoTaiChinh.LoaiTien == 2)
                    LoaiTien_Search = "%2%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuy(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeStart, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search,
                    ID_NhomDoiTuong_search, ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search, LoaiTien_Search);
                double TonDauKy = 0;
                if (lst.Count() != 0)
                {
                    TonDauKy = (double?)lst.FirstOrDefault().TonDauKy ?? 0;
                }
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyPRC>(lst);
                excel.Columns.Remove("TonDauKy");
                excel.Columns.Remove("TienMat");
                excel.Columns.Remove("TienGui");
                excel.Columns.Remove("ThuTienMat");
                excel.Columns.Remove("ChiTienMat");
                excel.Columns.Remove("ThuTienGui");
                excel.Columns.Remove("ChiTienGui");
                excel.Columns.Remove("TonLuyKeTienGui");
                excel.Columns.Remove("TonLuyKeTienMat");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoSoQuyTongQuy.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyTongQuy.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_StypeSQ(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, array_BaoCaoTaiChinh.TenChiNhanh, 9, TonDauKy);
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoSoQuyTongQuy.xlsx");
                return fileSave;
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage Export_BCTC_SoQuyTheoChiNhanh_v2([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                string textSearch = "";

                DateTime timeEnd = DateTime.Now;
                if (objIn["TimeEnd"] != null)
                {
                    string tempDate = objIn["TimeEnd"].ToObject<string>();
                    timeEnd = DateTime.ParseExact(tempDate, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    //timeEnd = objIn["TimeEnd"].ToObject<DateTime>();
                }
                string LoaiKH = "";
                if (objIn["LoaiKH"] != null && objIn["LoaiKH"].ToObject<List<int>>().Count > 0)
                {
                    LoaiKH = string.Join(",", objIn["LoaiKH"].ToObject<List<int>>());
                }
                string IdNhomDoiTuong = "";
                string IdNhomKhachHang = "";
                if (objIn["IdNhomKhachHang"] != null && objIn["IdNhomKhachHang"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomKhachHang = string.Join(",", objIn["IdNhomKhachHang"].ToObject<List<string>>());
                }
                string IdNhomNhaCungCap = "";
                if (objIn["IdNhomNhaCungCap"] != null && objIn["IdNhomNhaCungCap"].ToObject<List<string>>().Count > 0)
                {
                    IdNhomNhaCungCap = string.Join(",", objIn["IdNhomNhaCungCap"].ToObject<List<string>>());
                }
                if (IdNhomKhachHang != "" && IdNhomNhaCungCap != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang + "," + IdNhomNhaCungCap;
                }
                else if (IdNhomKhachHang != "")
                {
                    IdNhomDoiTuong = IdNhomKhachHang;
                }
                else
                {
                    IdNhomDoiTuong = IdNhomNhaCungCap;
                }
                string LoaiThuChi = "";
                if (objIn["LoaiThuChi"] != null && objIn["LoaiThuChi"].ToObject<List<int>>().Count > 0)
                {
                    LoaiThuChi = string.Join(",", objIn["LoaiThuChi"].ToObject<List<int>>());
                }
                bool? HachToanKinhDoanh = null;
                if (objIn["HachToanKinhDoanh"] != null && objIn["HachToanKinhDoanh"].ToObject<string>() != "")
                {
                    HachToanKinhDoanh = objIn["HachToanKinhDoanh"].ToObject<bool>();
                }
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC> lst = new List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>();

                    lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuyTheoChiNhanh_v2(timeEnd, IdChiNhanh, LoaiKH, IdNhomDoiTuong, LoaiThuChi, HachToanKinhDoanh);
                    DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>(lst);
                    excel.Columns.Remove("ID");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoSoQuyTheoChiNhanh.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }


        [AcceptVerbs("GET", "POST")]
        public string Export_BCTC_SoQuyTheoChiNhanh([FromBody] JObject data)
        {
            array_BaoCaoTaiChinh array_BaoCaoTaiChinh = data["objExcel"].ToObject<array_BaoCaoTaiChinh>();
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC> lst = new List<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>();
                string ID_NhomDoiTuong = string.Empty;
                string ID_NhomDoiTuong_SP = string.Empty;
                string ID_NhomDoiTuong_search = "%%";
                if (array_BaoCaoTaiChinh.ID_NhomKhachHang != null && array_BaoCaoTaiChinh.ID_NhomKhachHang != string.Empty)
                {
                    ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomKhachHang.Replace(",null", "");
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                        ID_NhomDoiTuong = ID_NhomDoiTuong + "," + array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                    ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                    ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                }
                else
                {
                    if (array_BaoCaoTaiChinh.ID_NhomNhaCungCap != null && array_BaoCaoTaiChinh.ID_NhomNhaCungCap != string.Empty)
                    {
                        ID_NhomDoiTuong = array_BaoCaoTaiChinh.ID_NhomNhaCungCap.Replace(",null", "");
                        ID_NhomDoiTuong_search = "%" + ID_NhomDoiTuong + "%";
                        ID_NhomDoiTuong_SP = ID_NhomDoiTuong;
                    }
                    else
                        ID_NhomDoiTuong_SP = Guid.NewGuid().ToString();
                }
                string loaiThuChi_search = array_BaoCaoTaiChinh.LoaiThuChi.Replace(",null", "");
                string maKH_search = "%%";
                string LaDT_search = "%%";
                string maKH_TV = "%%";
                if (array_BaoCaoTaiChinh.MaHangHoa != null & array_BaoCaoTaiChinh.MaHangHoa != "" & array_BaoCaoTaiChinh.MaHangHoa != "null")
                {
                    maKH_search = "%" + CommonStatic.ConvertToUnSign(array_BaoCaoTaiChinh.MaHangHoa).ToLower() + "%";
                    maKH_TV = "%" + array_BaoCaoTaiChinh.MaHangHoa.Trim() + "%";
                }
                if (array_BaoCaoTaiChinh.LoaiDoiTuong == 0)
                    LaDT_search = "%2%";
                else if (array_BaoCaoTaiChinh.LoaiDoiTuong == 1)
                    LaDT_search = "%1%";
                string HachToanKD_Search = "%%";
                if (array_BaoCaoTaiChinh.HachToanKD == 1)
                    HachToanKD_Search = "%1%";
                if (array_BaoCaoTaiChinh.HachToanKD == 2)
                    HachToanKD_Search = "%0%";
                var idChiNhanhs = string.Join(",", array_BaoCaoTaiChinh.lstIDChiNhanh);
                lst = reportTaiChinh.GetBaoCaoTaiChinh_SoQuyTheoChiNhanh(maKH_search, maKH_TV, array_BaoCaoTaiChinh.timeEnd, idChiNhanhs, LaDT_search, ID_NhomDoiTuong_search,
                    ID_NhomDoiTuong_SP, loaiThuChi_search, HachToanKD_Search);
                DataTable excel = classOffice.ToDataTable<BaoCaoTaiChinh_SoQuyTheoChiNhanhPRC>(lst);
                excel.Columns.Remove("ID");
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoSoQuyTheoChiNhanh.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoSoQuyTheoChiNhanh.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, true, array_BaoCaoTaiChinh.columnsHide.Trim(), array_BaoCaoTaiChinh.TodayBC, "");
                HttpResponse Response = HttpContext.Current.Response;
                fileSave = classOffice.createFolder_Export("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoSoQuyTheoChiNhanh.xlsx");
                return fileSave;
            }
        }
        [HttpGet]
        public void Export_BCTC_TheoNam(int year, string ID_ChiNhanh, string columnsHide, string TodayBC, string TenChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<Report_TaiChinh_TheoNam> lst = reportTaiChinh.getListTaiChinh_TheoNam(year, ID_ChiNhanh);
                DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoNam>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoTaiChinhTheoNam.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoTaiChinhTheoNam.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, false, columnsHide, TodayBC, TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                classOffice.downloadFile(fileSave);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportBaoCaoKetQuaHoatDongKinhDoanhTheoThang([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                List<Report_TaiChinh_TheoThang> lst = new List<Report_TaiChinh_TheoThang>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    string fileTeamplate = "";
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoThang_Gara(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoThang_Gara.xlsx");
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoThang(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoThang.xlsx");
                    }
                    DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoThang>(lst);
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, false, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportBaoCaoKetQuaHoatDongKinhDoanhTheoQuy([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                List<Report_TaiChinh_TheoQuy> lst = new List<Report_TaiChinh_TheoQuy>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    string fileTeamplate = "";
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoQuy_Gara(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoQuy_Gara.xlsx");
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoQuy(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoQuy.xlsx");
                    }
                    DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoQuy>(lst);

                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, false, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportBaoCaoKetQuaHoatDongKinhDoanhTheoNam([FromBody] JObject objIn)
        {
            try
            {
                int Year = DateTime.Now.Year;
                if (objIn["Year"] != null)
                    Year = objIn["Year"].ToObject<int>();
                string IdChiNhanh = "";
                if (objIn["IdChiNhanhs"] != null && objIn["IdChiNhanhs"].ToObject<List<string>>().Count > 0)
                    IdChiNhanh = string.Join(",", objIn["IdChiNhanhs"].ToObject<List<string>>());
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                List<Report_TaiChinh_TheoNam> lst = new List<Report_TaiChinh_TheoNam>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    lst = reportTaiChinh.getListTaiChinh_TheoNam(Year, IdChiNhanh);
                    string NganhNgheKinhDoanh = CookieStore.GetCookieAes("shop").ToUpper();
                    string fileTeamplate = "";
                    if (NganhNgheKinhDoanh == "C16EDDA0-F6D0-43E1-A469-844FAB143014")
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoNam_Gara(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoNam_Gara.xlsx");
                    }
                    else
                    {
                        lst = reportTaiChinh.getListTaiChinh_TheoNam(Year, IdChiNhanh);
                        fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoKetQuaKinhDoanhTheoNam.xlsx");
                    }
                    DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoNam>(lst);
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, "Năm " + Year);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, false, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [HttpGet]
        public void Export_BCTC_TheoThang(int year, string ID_ChiNhanh, string columnsHide, string TodayBC, string TenChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<Report_TaiChinh_TheoThang> lst = reportTaiChinh.getListTaiChinh_TheoThang(year, ID_ChiNhanh);
                DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoThang>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoTaiChinhTheoThang.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoTaiChinhTheoThang.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, false, columnsHide, TodayBC, TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                classOffice.downloadFile(fileSave);
            }
        }
        [HttpGet]
        public void Export_BCTC_TheoQuy(int year, string ID_ChiNhanh, string columnsHide, string TodayBC, string TenChiNhanh)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                db.Database.CommandTimeout = 60 * 60;
                ClassReportTaiChinh reportTaiChinh = new ClassReportTaiChinh(db);
                Class_officeDocument classOffice = new Class_officeDocument(db);
                List<Report_TaiChinh_TheoQuy> lst = reportTaiChinh.getListTaiChinh_TheoQuy(year, ID_ChiNhanh);
                DataTable excel = classOffice.ToDataTable<Report_TaiChinh_TheoQuy>(lst);
                string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/Teamplate_BaoCaoTaiChinhTheoQuy.xlsx");
                string fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoTaiChinh/BaoCaoTaiChinhTheoQuy.xlsx");
                fileSave = classOffice.createFolder_Download(fileSave);
                classOffice.listToOfficeExcel_Stype(fileTeamplate, fileSave, excel, 4, 28, 24, false, columnsHide, TodayBC, TenChiNhanh);
                HttpResponse Response = HttpContext.Current.Response;
                classOffice.downloadFile(fileSave);
            }
        }

        #endregion

        #region "Báo cáo thẻ giá trị"
        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public IHttpActionResult ReportBalance_ValueCard(ParamReportValueCard lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                    List<SP_ReportValueCard_Balance> data = reportTheGiaTri.GetReportBalance_ValueCard(lstParam);
                    if (data.Count() > 0)
                    {
                        var itFirst = data[0];
                        return Json(new
                        {
                            res = true,
                            data = data,
                            TotalRow = itFirst.TotalRow,
                            TotalPage = itFirst.TotalPage,
                            TongTang = itFirst.TongPhatSinhTang,
                            TongGiam = itFirst.TongPhatSinhGiam,
                            TongDuDau = itFirst.TongSoDuDauKy,
                            TongDuCuoi = itFirst.TongSoDuCuoiKy,
                        });

                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            data = data,
                            TotalRow = 0,
                            TotalPage = 0,
                            TongTang = 0,
                            TongGiam = 0,
                            TongDuDau = 0,
                            TongDuCuoi = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public IHttpActionResult ReportDiary_ValueCard(ParamReportValueCard lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                List<SP_ReportValueCard_HisUsed> data = reportTheGiaTri.GetReportDiary_ValueCard(lstParam);

                if (data.Count() > 0)
                {
                    var itFirst = data[0];
                    return Json(new
                    {
                        res = true,
                        data = data,
                        TotalRow = itFirst.TotalRow,
                        TotalPage = itFirst.TotalPage,
                        TongTang = itFirst.TongPhatSinhTang,
                        TongGiam = itFirst.TongPhatSinhGiam,
                        TongDuDau = itFirst.TongSoDuDauKy,
                        TongDuCuoi = itFirst.TongSoDuCuoiKy,
                    });
                }
                else
                {
                    return Json(new
                    {
                        res = true,
                        data = data,
                        TotalRow = 0,
                        TotalPage = 0,
                        TongTang = 0,
                        TongGiam = 0,
                        TongDuDau = 0,
                        TongDuCuoi = 0,
                    });
                }
            }
        }

        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public IHttpActionResult ReportServiceUsed_ValueCard(ParamReportValueCard lstParam)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                int currentPage = lstParam.CurrentPage;
                int pageSize = lstParam.PageSize;

                List<SP_ValueCard_ServiceUsed> data = reportTheGiaTri.SP_ValueCard_ServiceUsed(lstParam);
                var result = data.GroupBy(o =>
                new
                {
                    o.ID_HoaDon,
                    o.ID_PhieuThuChi,
                    o.NgayLapHoaDon,
                    o.MaDoiTuong,
                    o.TenDoiTuong,
                    o.SLoaiHoaDon,
                    o.MaHoaDon,
                    o.MaPhieuThu,
                    o.PhatSinhGiam,
                    o.PhatSinhTang,
                }).Select(o => new
                {
                    ID_HoaDon = o.Key.ID_HoaDon,
                    ID_PhieuThuChi = o.Key.ID_PhieuThuChi,
                    NgayLapHoaDon = o.Key.NgayLapHoaDon,
                    MaDoiTuong = o.Key.MaDoiTuong,
                    TenDoiTuong = o.Key.TenDoiTuong,
                    SLoaiHoaDon = o.Key.SLoaiHoaDon,
                    MaHoaDon = o.Key.MaHoaDon,
                    MaPhieuThu = o.Key.MaPhieuThu,
                    PhatSinhGiam = o.Key.PhatSinhGiam,
                    PhatSinhTang = o.Key.PhatSinhTang,
                    LstHangHoas = o.ToList()
                }).AsEnumerable();
                int totalRow = result.Count();
                double totalPage = Math.Ceiling(totalRow * 1.0 / pageSize);
                double? giam = result.Sum(x => x.PhatSinhGiam);
                double? tang = result.Sum(x => x.PhatSinhTang);

                return Json(new
                {
                    res = true,
                    data = result.Skip(currentPage * pageSize).Take(pageSize).ToList(),
                    TotalRow = totalRow,
                    TotalPage = totalPage,
                    TongGiam = giam,
                    TongTang = tang,
                });
            }
        }

        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_ValueCard_Balance(ParamReportValueCard lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<SP_ReportValueCard_Balance> lst = reportTheGiaTri.GetReportBalance_ValueCard(lstParam);
                    DataTable excel = classOffice.ToDataTable<SP_ReportValueCard_Balance>(lst);
                    excel.Columns.Remove("ID");
                    excel.Columns.Remove("TongSoDuDauKy");
                    excel.Columns.Remove("TongPhatSinhTang");
                    excel.Columns.Remove("TongPhatSinhGiam");
                    excel.Columns.Remove("TongSoDuCuoiKy");
                    excel.Columns.Remove("TotalRow");
                    excel.Columns.Remove("TotalPage");

                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCao/" + "Teamplate_BaoCaoSoDu_TheGiaTri.xlsx");
                    var valExcel1 = string.Empty;
                    if (lstParam.DateFrom == "20160101")
                    {
                        valExcel1 = "Toàn thời gian";
                    }
                    else
                    {
                        valExcel1 = lstParam.DateFrom.ToString() + " - " + lstParam.DateTo.ToString();
                    }
                    var lstDataCell = classAposeCell.GetData_ForDefaultCellv2(lstParam.TextReport, valExcel1);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 6, 30, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                CookieStore.WriteLog("ExportExcel_ValueCard_Balance " + ex.InnerException + ex.Message);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }

        }

        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_ValueCard_HisUsed(ParamReportValueCard lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<SP_ReportValueCard_HisUsed> lst = reportTheGiaTri.GetReportDiary_ValueCard(lstParam);

                    DataTable excel = classOffice.ToDataTable<SP_ReportValueCard_HisUsed>(lst);
                    excel.Columns.Remove("ID");
                    excel.Columns.Remove("ID_DoiTuong");
                    excel.Columns.Remove("LoaiHoaDonSQ");
                    excel.Columns.Remove("TienThe");

                    excel.Columns.Remove("TongSoDuDauKy");
                    excel.Columns.Remove("TongPhatSinhTang");
                    excel.Columns.Remove("TongPhatSinhGiam");
                    excel.Columns.Remove("TongSoDuCuoiKy");
                    excel.Columns.Remove("TotalRow");
                    excel.Columns.Remove("TotalPage");

                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCao/" + "Teamplate_NhatKySuDung_TheGiaTri.xlsx");
                    string valExcel1 = string.Empty;
                    if (lstParam.DateFrom == "20160101")
                    {
                        valExcel1 = "Toàn thời gian";
                    }
                    else
                    {
                        valExcel1 = lstParam.DateFrom.ToString() + " - " + lstParam.DateTo.ToString();
                    }
                    var lstDataCell = classAposeCell.GetData_ForDefaultCellv2(lstParam.TextReport, valExcel1);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 6, 31, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                CookieStore.WriteLog("ExportExcel_ValueCard_HisUsed " + ex.InnerException + ex.Message);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }

        }

        [System.Web.Http.AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_ValueCard_ServiceUsed(ParamReportValueCard lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportTheGiaTri reportTheGiaTri = new ClassReportTheGiaTri(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    List<SP_ValueCard_ServiceUsed> lst = reportTheGiaTri.SP_ValueCard_ServiceUsed(lstParam);

                    DataTable excel = classOffice.ToDataTable<SP_ValueCard_ServiceUsed>(lst);
                    excel.Columns.Remove("ID_HoaDon");
                    excel.Columns.Remove("ID_PhieuThuChi");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCao/" + "Teamplate_NhatKySuDungDichVu_TheGiaTri.xlsx");
                    string valExcel1 = string.Empty;
                    if (lstParam.DateFrom == "20160101")
                    {
                        valExcel1 = "Toàn thời gian";
                    }
                    else
                    {
                        valExcel1 = lstParam.DateFrom.ToString() + " - " + lstParam.DateTo.ToString();
                    }

                    var lstDataCell = classAposeCell.GetData_ForDefaultCellv2(lstParam.TextReport, valExcel1);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 6, 30, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                CookieStore.WriteLog("ExportExcel_ValueCard_HisUsed " + ex.InnerException + ex.Message);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }
        #endregion

        #region "Báo cáo hoa hồng hàng hóa + hóa đơn + doanh thu"
        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountProduct_General(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<BaoCaoChietKhau_TongHopPRC> lst = reportHoaHong.SP_ReportDiscountProduct_General(lstParam);
                    if (lst.Count() > 0)
                    {
                        var firsRow = lst[0];
                        return Json(new
                        {
                            res = true,
                            LstData = lst,
                            TotalRow = firsRow.TotalRow,
                            TotalPage = firsRow.TotalPage,
                            SumThucHien = firsRow.TongHoaHongThucHien,
                            SumThucHien_TheoYC = firsRow.TongHoaHongThucHien_TheoYC,
                            SumTuVan = firsRow.TongHoaHongTuVan,
                            SumBanGoi = firsRow.TongHoaHongBanGoiDV,
                            SumAll = firsRow.TongAll,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<BaoCaoChietKhau_TongHopPRC>(),
                            TotalRow = 0,
                            TotalPage = 0,
                            SumThucHien = 0,
                            SumThucHien_TheoYC = 0,
                            SumTuVan = 0,
                            SumBanGoi = 0,
                            SumAll = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountProduct_Detail(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<BaoCaoChietKhau_ChiTietPRC> lst = reportHoaHong.SP_ReportDiscountProduct_Detail(lstParam);
                    if (lst.Count() > 0)
                    {
                        var firstRow = lst[0];
                        var lstGr = lst.GroupBy(x => new
                        {
                            x.MaHoaDon,
                            x.ID_ChiTietHoaDon,
                            x.NgayLapHoaDon,
                            x.MaKhachHang,
                            x.TenKhachHang,
                            x.DienThoaiKH,
                            x.TenNhomHangHoa,
                            x.MaHangHoa,
                            x.TenHangHoa,
                            x.TenHangHoaFull,
                            x.TenDonViTinh,
                            x.TenLoHang,
                            x.SoLuong,
                            x.ThanhTien,
                            x.GtriSauHeSo
                        }).Select(x => new
                        {
                            x.Key.MaHoaDon,
                            x.Key.ID_ChiTietHoaDon,
                            x.Key.NgayLapHoaDon,
                            x.Key.MaKhachHang,
                            x.Key.TenKhachHang,
                            x.Key.DienThoaiKH,
                            x.Key.TenNhomHangHoa,
                            x.Key.MaHangHoa,
                            x.Key.TenHangHoa,
                            x.Key.TenHangHoaFull,
                            x.Key.TenDonViTinh,
                            x.Key.TenLoHang,
                            x.Key.SoLuong,
                            x.Key.ThanhTien,
                            x.Key.GtriSauHeSo,
                            lstHoaHong = x,
                        });
                        return Json(new
                        {
                            res = true,
                            LstData = lstGr,
                            TotalRow = firstRow.TotalRow,
                            TotalPage = firstRow.TotalPage,
                            SumThucHien = firstRow.TongHoaHongThucHien,
                            SumThucHien_TheoYC = firstRow.TongHoaHongThucHien_TheoYC,
                            SumTuVan = firstRow.TongHoaHongTuVan,
                            SumBanGoi = firstRow.TongHoaHongBanGoiDV,
                            TongAll = firstRow.TongAllAll,
                            TongSoLuong = firstRow.TongSoLuong,
                            SumGtriCK = firstRow.TongThanhTien,
                            SumThanhTienSauHS = firstRow.TongThanhTienSauHS
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<BaoCaoChietKhau_ChiTietPRC>(),
                            TotalRow = 0,
                            TotalPage = 0,
                            TongSoLuong = 0,
                            SumThucHien = 0,
                            SumThucHien_TheoYC = 0,
                            SumTuVan = 0,
                            SumBanGoi = 0,
                            TongAll = 0,
                            SumGtriCK = 0
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountInvoice_General(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountInvoice_General> data = reportHoaHong.SP_ReportDiscountInvoice_General(lstParam);

                    if (data.Count() > 0)
                    {
                        var firstRow = data[0];
                        return Json(new
                        {
                            res = true,
                            LstData = data,
                            SumDoanhThu = firstRow.TongHoaHongDoanhThu,
                            SumThucThu = firstRow.TongHoaHongThucThu,
                            SumVND = firstRow.TongHoaHongVND,
                            SumAll = firstRow.TongAllAll,

                            TotalRow = firstRow.TotalRow,
                            TotalPage = firstRow.TotalPage,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<SP_ReportDiscountInvoice_General>(),
                            SumDoanhThu = 0,
                            SumThucThu = 0,
                            SumVND = 0,
                            SumAll = 0,

                            TotalRow = 0,
                            TotalPage = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountInvoice_Detail(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountInvoice_Detail> data = reportHoaHong.SP_ReportDiscountInvoice_Detail(lstParam);
                    if (data.Count() > 0)
                    {
                        var firstRow = data[0];
                        var lstGr = data.GroupBy(x => new
                        {
                            x.ID,
                            x.MaHoaDon,
                            x.NgayLapHoaDon,
                            x.MaKhachHang,
                            x.TenKhachHang,
                            x.DienThoaiKH,
                            x.DoanhThu,
                        })
                            .Select(x => new
                            {
                                x.Key.ID,
                                x.Key.MaHoaDon,
                                x.Key.NgayLapHoaDon,
                                x.Key.MaKhachHang,
                                x.Key.TenKhachHang,
                                x.Key.DienThoaiKH,
                                x.Key.DoanhThu,
                                lstHoaHong = x,
                            });
                        return Json(new
                        {
                            res = true,
                            LstData = lstGr,

                            DoanhThuHD = firstRow.TongDoanhThu,
                            TongThucThuHD = firstRow.TongThucThu,
                            SumDoanhThu = firstRow.TongHoaHongDoanhThu,
                            SumThucThu = firstRow.TongHoaHongThucThu,
                            SumVND = firstRow.TongHoaHongVND,
                            SumAll = firstRow.TongAllAll,

                            TotalRow = firstRow.TotalRow,
                            TotalPage = firstRow.TotalPage,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<SP_ReportDiscountInvoice_Detail>(),

                            DoanhThuHD = 0,
                            SumDoanhThu = 0,
                            SumThucThu = 0,
                            SumVND = 0,
                            SumAll = 0,

                            TotalRow = 0,
                            TotalPage = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountSales(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountSales> data = reportHoaHong.SP_ReportDiscountSales(lstParam);

                    if (data.Count() > 0)
                    {
                        var firstRow = data[0];

                        return Json(new
                        {
                            res = true,
                            LstData = data,

                            SumHHDoanhThu = firstRow.TongHoaHongDoanhThu,
                            SumHHThucThu = firstRow.TongHoaHongThucThu,
                            SumDoanhThu = firstRow.TongDoanhThu,
                            SumThucThu = firstRow.TongThucThu,
                            SumAll = firstRow.TongAllAll,

                            firstRow.TotalRow,
                            firstRow.TotalPage,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<SP_ReportDiscountSales>(),

                            SumHHDoanhThu = 0,
                            SumHHThucThu = 0,
                            SumDoanhThu = 0,
                            SumThucThu = 0,
                            SumAll = 0,

                            TotalRow = 0,
                            TotalPage = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountSales_Detail(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountSales_Detail> data = reportHoaHong.DiscountSale_byIDNhanVien(lstParam);

                    int pageSise = lstParam.PageSize;
                    int totalRow = data.Count();
                    double totalPage = Math.Ceiling(totalRow * 1.0 / pageSise);

                    return Json(new
                    {
                        res = true,
                        LstData = data.Skip(lstParam.CurrentPage * pageSise).Take(pageSise).ToList(),
                        TotalRow = totalRow,
                        TotalPage = totalPage,
                    });
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ReportDiscountAll(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountAll> data = reportHoaHong.SP_ReportDiscountAll(lstParam);
                    if (data.Count() > 0)
                    {
                        var firstRow = data[0];

                        return Json(new
                        {
                            res = true,
                            LstData = data,

                            TongHHThucHien = firstRow.TongHoaHongThucHien,
                            TongHHThucHien_TheoYC = firstRow.TongHoaHongThucHien_TheoYC,
                            TongHHTuVan = firstRow.TongHoaHongTuVan,
                            TongHHBanGoiDV = firstRow.TongHoaHongBanGoiDV,
                            TongHH_HangHoa = firstRow.TongHoaHong_TheoHangHoa,

                            TongHHDoanhThu = firstRow.TongHoaHongDoanhThu,
                            TongHHThucThu = firstRow.TongHoaHongThucThu,
                            TongHHVND = firstRow.TongHoaHongVND,
                            TongHH_HoaDon = firstRow.TongHoaHong_TheoHoaDon,
                            TongDoanhThu = firstRow.TongDoanhThu,
                            TongThucThu = firstRow.TongThucThu,

                            TongHHDoanhThuDS = firstRow.TongHoaHongDoanhThuDS,
                            TongHHThucThuDS = firstRow.TongHoaHongThucThuDS,
                            TongHH_DoanhSo = firstRow.TongHoaHong_TheoDoanhSo,
                            TongHH_All = firstRow.TongHoaHongAll,

                            TotalRow = firstRow.TotalRow,
                            TotalPage = firstRow.TotalPage,
                        });
                    }
                    else
                    {
                        return Json(new
                        {
                            res = true,
                            LstData = new List<SP_ReportDiscountAll>(),

                            TongHHThucHien = 0,
                            TongHHThucHien_TheoYC = 0,
                            TongHHTuVan = 0,
                            TongHHBanGoiDV = 0,
                            TongHH_HangHoa = 0,

                            TongHHDoanhThu = 0,
                            TongHHThucThu = 0,
                            TongHHVND = 0,
                            TongHH_HoaDon = 0,
                            TongDoanhThu = 0,
                            TongThucThu = 0,

                            TongHHDoanhThuDS = 0,
                            TongHHThucThuDS = 0,
                            TongHH_DoanhSo = 0,
                            TongHH_All = 0,

                            TotalRow = 0,
                            TotalPage = 0,
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                return Json(new
                {
                    res = false,
                    mes = ex.InnerException + ex.Message
                });
            }
        }


        [HttpGet, HttpPost]
        public HttpResponseMessage ExportExcel_ReportDiscountProduct(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    string fileTeamplate = string.Empty;
                    DataTable excel = null;
                    switch (lstParam.TypeReport)
                    {
                        case 1:
                            List<BaoCaoChietKhau_TongHopPRC> lst = reportHoaHong.SP_ReportDiscountProduct_General(lstParam);
                            excel = classOffice.ToDataTable<BaoCaoChietKhau_TongHopPRC>(lst);
                            excel.Columns.Remove("TongHoaHongThucHien");
                            excel.Columns.Remove("TongHoaHongThucHien_TheoYC");
                            excel.Columns.Remove("TongHoaHongTuVan");
                            excel.Columns.Remove("TongHoaHongBanGoiDV");
                            excel.Columns.Remove("TongAll");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Teamplate_BaoCaoTongHopHoaHongNhanVien.xlsx");
                            break;
                        case 2:
                            List<BaoCaoChietKhau_ChiTietPRC> lstCT = reportHoaHong.SP_ReportDiscountProduct_Detail(lstParam);
                            excel = classOffice.ToDataTable<BaoCaoChietKhau_ChiTietPRC>(lstCT);
                            excel.Columns.Remove("TenHangHoa");
                            excel.Columns.Remove("TongSoLuong");
                            excel.Columns.Remove("ThuocTinh_GiaTri");
                            excel.Columns.Remove("TongHoaHongThucHien");
                            excel.Columns.Remove("TongHoaHongThucHien_TheoYC");
                            excel.Columns.Remove("TongHoaHongTuVan");
                            excel.Columns.Remove("TongHoaHongBanGoiDV");
                            excel.Columns.Remove("TongAllAll");
                            excel.Columns.Remove("TongThanhTien");
                            excel.Columns.Remove("TongThanhTienSauHS");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            excel.Columns.Remove("ID_HoaDon");
                            excel.Columns.Remove("ID_DonViQuiDoi");
                            excel.Columns.Remove("ID_ChiTietHoaDon");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongHangHoa_ChiTiet.xlsx");
                            break;
                    }

                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(lstParam.TextReport, lstParam.TodayBC);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [HttpGet, HttpPost]
        public HttpResponseMessage ExportExcel_ReportDiscountInvoice(ParamReportDiscount lstParam)
        {
            List<BaoCaoChietKhau_TongHopPRC> lst = new List<BaoCaoChietKhau_TongHopPRC>();
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    string fileTeamplate = string.Empty;
                    DataTable excel = null;

                    switch (lstParam.TypeReport)
                    {
                        case 3:
                            List<SP_ReportDiscountInvoice_General> data = reportHoaHong.SP_ReportDiscountInvoice_General(lstParam);
                            excel = classOffice.ToDataTable<SP_ReportDiscountInvoice_General>(data);
                            excel.Columns.Remove("TongHoaHongDoanhThu");
                            excel.Columns.Remove("TongHoaHongThucThu");
                            excel.Columns.Remove("TongHoaHongVND");
                            excel.Columns.Remove("TongAllAll");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongHoaDon.xlsx");
                            break;
                        case 4:
                            List<SP_ReportDiscountInvoice_Detail> data2 = reportHoaHong.SP_ReportDiscountInvoice_Detail(lstParam);
                            excel = classOffice.ToDataTable<SP_ReportDiscountInvoice_Detail>(data2);
                            excel.Columns.Remove("ID");
                            excel.Columns.Remove("TongDoanhThu");
                            excel.Columns.Remove("TongThucThu");
                            excel.Columns.Remove("TongHoaHongDoanhThu");
                            excel.Columns.Remove("TongHoaHongThucThu");
                            excel.Columns.Remove("TongHoaHongVND");
                            excel.Columns.Remove("TongAllAll");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongHoaDon_ChiTiet.xlsx");
                            break;
                    }
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(lstParam.TextReport, lstParam.TodayBC);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [HttpGet, HttpPost]
        public HttpResponseMessage ExportExcel_ReportDiscountSales(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    string fileTeamplate = string.Empty;
                    DataTable excel = null;

                    switch (lstParam.TypeReport)
                    {
                        case 5:
                            List<SP_ReportDiscountSales> data = reportHoaHong.SP_ReportDiscountSales(lstParam);
                            excel = classOffice.ToDataTable<SP_ReportDiscountSales>(data);
                            excel.Columns.Remove("ID_NhanVien");
                            excel.Columns.Remove("TongDoanhThu");
                            excel.Columns.Remove("TongThucThu");
                            excel.Columns.Remove("TongHoaHongDoanhThu");
                            excel.Columns.Remove("TongHoaHongThucThu");
                            excel.Columns.Remove("TongAllAll");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongDoanhThu.xlsx");
                            break;
                        case 6:
                            List<SP_ReportDiscountSales_Detail> data2 = reportHoaHong.DiscountSale_byIDNhanVien(lstParam);
                            excel = classOffice.ToDataTable<SP_ReportDiscountSales_Detail>(data2);
                            excel.Columns.Remove("LaPhanTram");
                            excel.Columns.Remove("TongDoanhThu");
                            excel.Columns.Remove("TongThucThu");
                            excel.Columns.Remove("TongGiaTriChietKhau");
                            excel.Columns.Remove("TongAll");
                            excel.Columns.Remove("TotalRow");
                            excel.Columns.Remove("TotalPage");
                            fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongDoanhThu_ChiTiet.xlsx");
                            break;
                    }

                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(lstParam.TextReport, lstParam.TodayBC);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 4, 28, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [HttpGet, HttpPost]
        public HttpResponseMessage ExportExcel_ReportDiscountAll(ParamReportDiscount lstParam)
        {
            try
            {
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    db.Database.CommandTimeout = 60 * 60;
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    ClassReportHoaHong reportHoaHong = new ClassReportHoaHong(db);
                    List<SP_ReportDiscountAll> data = reportHoaHong.SP_ReportDiscountAll(lstParam);
                    DataTable excel = classOffice.ToDataTable<SP_ReportDiscountAll>(data);

                    excel.Columns.Remove("TongHoaHongThucHien");
                    excel.Columns.Remove("TongHoaHongThucHien_TheoYC");
                    excel.Columns.Remove("TongHoaHongTuVan");
                    excel.Columns.Remove("TongHoaHongBanGoiDV");
                    excel.Columns.Remove("TongHoaHong_TheoHangHoa");

                    excel.Columns.Remove("TongHoaHongDoanhThu");
                    excel.Columns.Remove("TongHoaHongThucThu");
                    excel.Columns.Remove("TongHoaHongVND");
                    excel.Columns.Remove("TongHoaHong_TheoHoaDon");
                    excel.Columns.Remove("TongDoanhThu");
                    excel.Columns.Remove("TongThucThu");

                    excel.Columns.Remove("TongHoaHongDoanhThuDS");
                    excel.Columns.Remove("TongHoaHongThucThuDS");
                    excel.Columns.Remove("TongHoaHong_TheoDoanhSo");
                    excel.Columns.Remove("TongHoaHongAll");

                    excel.Columns.Remove("TotalRow");
                    excel.Columns.Remove("TotalPage");

                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Report/BaoCaoChietKhau/Temp_BaoCaoHoaHongNhanVien_All.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(lstParam.TextReport, lstParam.TodayBC);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 29, true, lstParam.ColumnsHide, lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        #endregion

        #region ExportExcel Gara

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_GetBaoCaoDoanhThuSuaChuaTongHop([FromBody] JObject objIn)
        {
            try
            {
                string fileSave = string.Empty;
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTongHop> lstResult = new List<BaoCaoDoanhThuSuaChuaTongHop>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTongHop(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                    List<BaoCaoDoanhThuSuaChuaTongHop_Export> lst = lstResult.Select(p => new BaoCaoDoanhThuSuaChuaTongHop_Export
                    {
                        MaPhieuTiepNhan = p.MaPhieuTiepNhan,
                        NgayVaoXuong = p.NgayVaoXuong,
                        BienSo = p.BienSo,
                        MaDoiTuong = p.MaDoiTuong,
                        TenDoiTuong = p.TenDoiTuong,
                        CoVanDichVu = p.CoVanDichVu,
                        MaHoaDon = p.MaHoaDon,
                        NgayLapHoaDon = p.NgayLapHoaDon,
                        TongTienHang = p.TongTienHang,
                        TongChietKhau = p.TongChietKhau,
                        TongTienThue = p.TongTienThue,
                        TongGiamGia = p.TongGiamGia,
                        TongGiamTruBH = p.TongGiamTruBH,
                        DoanhThu = p.DoanhThu,
                        TienVon = p.TienVon,
                        TongChiPhi = p.TongChiPhi,
                        LoiNhuan = p.LoiNhuan,
                        GhiChu = p.GhiChu,
                        TenDonVi = p.TenDonVi,
                        SoKhung = p.SoKhung
                    }).ToList();
                    DataTable excel = classOffice.ToDataTable<BaoCaoDoanhThuSuaChuaTongHop_Export>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoDoanhThuSuaChuaTongHop.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 37, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                CookieStore.WriteLog("ExportExcel_GetBaoCaoDoanhThuSuaChuaTongHop " + ex.InnerException + ex.Message);
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_GetBaoCaoDoanhThuSuaChuaChiTiet([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                Guid? IdNhomHangHoa = null;
                if (objIn["IdNhomHangHoa"] != null && objIn["IdNhomHangHoa"].ToObject<string>() != "")
                    IdNhomHangHoa = objIn["IdNhomHangHoa"].ToObject<Guid>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaChiTiet> lstResult = new List<BaoCaoDoanhThuSuaChuaChiTiet>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaChiTiet(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, IdNhomHangHoa, TrangThai);
                    List<BaoCaoDoanhThuSuaChuaChiTiet_Export> lst = lstResult.Select(p => new BaoCaoDoanhThuSuaChuaChiTiet_Export
                    {
                        MaPhieuTiepNhan = p.MaPhieuTiepNhan,
                        NgayVaoXuong = p.NgayVaoXuong,
                        BienSo = p.BienSo,
                        MaDoiTuong = p.MaDoiTuong,
                        TenDoiTuong = p.TenDoiTuong,
                        CoVanDichVu = p.CoVanDichVu,
                        MaHoaDon = p.MaHoaDon,
                        NgayLapHoaDon = p.NgayLapHoaDon,
                        MaHangHoa = p.MaHangHoa,
                        TenHangHoa = p.TenHangHoa,
                        TenDonViTinh = p.TenDonViTinh,
                        SoLuong = p.SoLuong,
                        DonGia = p.DonGia,
                        TienChietKhau = p.TienChietKhau,
                        TienThue = p.TienThue,
                        ThanhTien = p.ThanhTien,
                        GiamGia = p.GiamGia,
                        GiamTruBH = p.GiamTruBH,
                        DoanhThu = p.DoanhThu,
                        TienVon = p.TienVon,
                        ChiPhi = p.ChiPhi,
                        LoiNhuan = p.LoiNhuan,
                        GhiChu = p.GhiChu,
                        TenDonVi = p.TenDonVi,
                        TenHangHoaThayThe = p.TenHangHoaThayThe,
                        SoKhung = p.SoKhung
                    }).ToList();
                    DataTable excel = classOffice.ToDataTable<BaoCaoDoanhThuSuaChuaChiTiet_Export>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoDoanhThuSuaChuaChiTiet.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 100, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_GetBaoCaoDoanhThuSuaChuaTheoXe([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                double? SoLanTiepNhanFrom = null;
                if (objIn["SoLanTiepNhanFrom"] != null && objIn["SoLanTiepNhanFrom"].ToObject<string>() != "")
                    SoLanTiepNhanFrom = objIn["SoLanTiepNhanFrom"].ToObject<double>();
                double? SoLanTiepNhanTo = null;
                if (objIn["SoLanTiepNhanTo"] != null && objIn["SoLanTiepNhanTo"].ToObject<string>() != "")
                    SoLanTiepNhanTo = objIn["SoLanTiepNhanTo"].ToObject<double>();
                double? SoLuongHoaDonFrom = null;
                if (objIn["SoLuongHoaDonFrom"] != null && objIn["SoLuongHoaDonFrom"].ToObject<string>() != "")
                    SoLuongHoaDonFrom = objIn["SoLuongHoaDonFrom"].ToObject<double>();
                double? SoLuongHoaDonTo = null;
                if (objIn["SoLuongHoaDonTo"] != null && objIn["SoLuongHoaDonTo"].ToObject<string>() != "")
                    SoLuongHoaDonTo = objIn["SoLuongHoaDonTo"].ToObject<double>();
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTheoXe> lstResult = new List<BaoCaoDoanhThuSuaChuaTheoXe>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTheoXe(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, SoLanTiepNhanFrom, SoLanTiepNhanTo, SoLuongHoaDonFrom, SoLuongHoaDonTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                    List<BaoCaoDoanhThuSuaChuaTheoXe_Export> lst = lstResult.Select(p => new BaoCaoDoanhThuSuaChuaTheoXe_Export
                    {
                        BienSo = p.BienSo,
                        SoKhung = p.SoKhung,
                        SoMay = p.SoMay,
                        TenDoiTuong = p.TenDoiTuong,
                        DienThoai = p.DienThoai,
                        SoLanTiepNhan = p.SoLanTiepNhan,
                        SoLuongHoaDon = p.SoLuongHoaDon,
                        TongGiamTruBH = p.TongGiamTruBH,
                        TongDoanhThu = p.TongDoanhThu,
                        TongTienVon = p.TongTienVon,
                        ChiPhi = p.ChiPhi,
                        LoiNhuan = p.LoiNhuan,
                        NgayGiaoDichGanNhat = p.NgayGiaoDichGanNhat,
                        TenDonVi = p.TenDonVi
                    }).ToList();
                    DataTable excel = classOffice.ToDataTable<BaoCaoDoanhThuSuaChuaTheoXe_Export>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoDoanhThuSuaChuaTheoXe.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 100, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public HttpResponseMessage ExportExcel_GetBaoCaoDoanhThuSuaChuaTheoCoVan([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                double? SoLanTiepNhanFrom = null;
                if (objIn["SoLanTiepNhanFrom"] != null && objIn["SoLanTiepNhanFrom"].ToObject<string>() != "")
                    SoLanTiepNhanFrom = objIn["SoLanTiepNhanFrom"].ToObject<double>();
                double? SoLanTiepNhanTo = null;
                if (objIn["SoLanTiepNhanTo"] != null && objIn["SoLanTiepNhanTo"].ToObject<string>() != "")
                    SoLanTiepNhanTo = objIn["SoLanTiepNhanTo"].ToObject<double>();
                double? SoLuongHoaDonFrom = null;
                if (objIn["SoLuongHoaDonFrom"] != null && objIn["SoLuongHoaDonFrom"].ToObject<string>() != "")
                    SoLuongHoaDonFrom = objIn["SoLuongHoaDonFrom"].ToObject<double>();
                double? SoLuongHoaDonTo = null;
                if (objIn["SoLuongHoaDonTo"] != null && objIn["SoLuongHoaDonTo"].ToObject<string>() != "")
                    SoLuongHoaDonTo = objIn["SoLuongHoaDonTo"].ToObject<double>();
                List<int> lstColHide = new List<int>();
                if (objIn["ColHide"] != null)
                    lstColHide = objIn["ColHide"].ToObject<List<int>>();
                string BaoCaoThoiGian = "";
                if (objIn["BaoCaoThoiGian"] != null && objIn["BaoCaoThoiGian"].ToObject<string>() != "")
                    BaoCaoThoiGian = objIn["BaoCaoThoiGian"].ToObject<string>();
                string BaoCaoChiNhanh = "";
                if (objIn["BaoCaoChiNhanh"] != null && objIn["BaoCaoChiNhanh"].ToObject<string>() != "")
                    BaoCaoChiNhanh = objIn["BaoCaoChiNhanh"].ToObject<string>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTheoCoVan> lstResult = new List<BaoCaoDoanhThuSuaChuaTheoCoVan>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    ClassAsposeExportExcel classAposeCell = new ClassAsposeExportExcel();
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTheoCoVan(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, SoLanTiepNhanFrom, SoLanTiepNhanTo, SoLuongHoaDonFrom, SoLuongHoaDonTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                    List<BaoCaoDoanhThuSuaChuaTheoCoVan_Export> lst = lstResult.Select(p => new BaoCaoDoanhThuSuaChuaTheoCoVan_Export
                    {
                        MaNhanVien = p.MaNhanVien,
                        TenNhanVien = p.TenNhanVien,
                        SoLanTiepNhan = p.SoLanTiepNhan,
                        SoLuongHoaDon = p.SoLuongHoaDon,
                        TongGiamTruBH = p.TongGiamTruBH,
                        TongDoanhThu = p.TongDoanhThu,
                        TongTienVon = p.TongTienVon,
                        ChiPhi = p.ChiPhi,
                        LoiNhuan = p.LoiNhuan,
                        NgayGiaoDichGanNhat = p.NgayGiaoDichGanNhat,
                        TenDonVi = p.TenDonVi
                    }).ToList();
                    DataTable excel = classOffice.ToDataTable<BaoCaoDoanhThuSuaChuaTheoCoVan_Export>(lst);
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoDoanhThuSuaChuaTheoCoVan.xlsx");
                    var lstDataCell = classAposeCell.GetData_ForDefaultCell(BaoCaoChiNhanh, BaoCaoThoiGian);
                    HttpResponseMessage response = classAposeCell.ExportData_ToOneSheet(fileTeamplate, excel, 5, 100, true, string.Join("_", lstColHide), lstDataCell);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.InnerException + ex.Message);
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult ExportExcel_BaoCaoHoatDongXe_TongHop(ParamRpHoatDongXe param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                string fileSave = string.Empty;
                try
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<BaoCaoHoatDongXe_TongHop> lst = reportGara.BaoCaoHoatDongXe_TongHop(param);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    DataTable excel = classOffice.ToDataTable<BaoCaoHoatDongXe_TongHop>(lst);
                    excel.Columns.Remove("ID_Xe");
                    excel.Columns.Remove("ID_DonVi");
                    excel.Columns.Remove("TongSoGioHoatDong");
                    excel.Columns.Remove("TotalRow");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoHoatDongXe_TongHop.xlsx");
                    fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCaoHoatDongXe_TongHop.xlsx");
                    fileSave = classOffice.createFolder_Download(fileSave);
                    classOffice.ListToOfficeExcel(fileTeamplate, fileSave, excel, 5, 100, 95, true, param.ReportText);

                    var index = fileSave.IndexOf(@"\Template");
                    fileSave = "~" + fileSave.Substring(index, fileSave.Length - index);
                    fileSave = fileSave.Replace(@"\", "/");
                    return ActionTrueNotData(fileSave);
                }
                catch (Exception ex)
                {
                    return ActionFalseNotData(ex.Message);
                }
            }
        }
        [HttpGet, HttpPost]
        public IHttpActionResult ExportExcel_BaoCaoHoatDongXe_ChiTiet(ParamRpHoatDongXe param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                string fileSave = string.Empty;
                try
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<BaoCaoHoatDongXe_ChiTiet> lst = reportGara.BaoCaoHoatDongXe_ChiTiet(param);
                    Class_officeDocument classOffice = new Class_officeDocument(db);
                    DataTable excel = classOffice.ToDataTable<BaoCaoHoatDongXe_ChiTiet>(lst);
                    excel.Columns.Remove("ID_PhieuTiepNhan");
                    excel.Columns.Remove("TongSoGioHoatDong");
                    excel.Columns.Remove("TotalRow");
                    string fileTeamplate = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/Gara/Template_BaoCaoHoatDongXe_ChiTiet.xlsx");
                    fileSave = HttpContext.Current.Server.MapPath("~/Template/ExportExcel/BaoCaoHoatDongXe_ChiTiet.xlsx");
                    fileSave = classOffice.createFolder_Download(fileSave);
                    classOffice.ListToOfficeExcel(fileTeamplate, fileSave, excel, 5, 100, 95, true, param.ReportText);

                    var index = fileSave.IndexOf(@"\Template");
                    fileSave = "~" + fileSave.Substring(index, fileSave.Length - index);
                    fileSave = fileSave.Replace(@"\", "/");
                    return ActionTrueNotData(fileSave);
                }
                catch (Exception ex)
                {
                    return ActionFalseNotData(ex.Message);
                }
            }
        }
        #endregion

        #region Report Gara
        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoDoanhThuSuaChuaTongHop([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTongHop> lstResult = new List<BaoCaoDoanhThuSuaChuaTongHop>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTongHop(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                }
                return ActionTrueData(new
                {
                    data = lstResult
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoDoanhThuSuaChuaChiTiet([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                Guid? IdNhomHangHoa = null;
                if (objIn["IdNhomHangHoa"] != null && objIn["IdNhomHangHoa"].ToObject<string>() != "")
                    IdNhomHangHoa = objIn["IdNhomHangHoa"].ToObject<Guid>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaChiTiet> lstResult = new List<BaoCaoDoanhThuSuaChuaChiTiet>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaChiTiet(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, IdNhomHangHoa, TrangThai);
                }
                return ActionTrueData(new
                {
                    data = lstResult
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoDoanhThuSuaChuaTheoXe([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                double? SoLanTiepNhanFrom = null;
                if (objIn["SoLanTiepNhanFrom"] != null && objIn["SoLanTiepNhanFrom"].ToObject<string>() != "")
                    SoLanTiepNhanFrom = objIn["SoLanTiepNhanFrom"].ToObject<double>();
                double? SoLanTiepNhanTo = null;
                if (objIn["SoLanTiepNhanTo"] != null && objIn["SoLanTiepNhanTo"].ToObject<string>() != "")
                    SoLanTiepNhanTo = objIn["SoLanTiepNhanTo"].ToObject<double>();
                double? SoLuongHoaDonFrom = null;
                if (objIn["SoLuongHoaDonFrom"] != null && objIn["SoLuongHoaDonFrom"].ToObject<string>() != "")
                    SoLuongHoaDonFrom = objIn["SoLuongHoaDonFrom"].ToObject<double>();
                double? SoLuongHoaDonTo = null;
                if (objIn["SoLuongHoaDonTo"] != null && objIn["SoLuongHoaDonTo"].ToObject<string>() != "")
                    SoLuongHoaDonTo = objIn["SoLuongHoaDonTo"].ToObject<double>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTheoXe> lstResult = new List<BaoCaoDoanhThuSuaChuaTheoXe>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTheoXe(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, SoLanTiepNhanFrom, SoLanTiepNhanTo, SoLuongHoaDonFrom, SoLuongHoaDonTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                }
                return ActionTrueData(new
                {
                    data = lstResult
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [AcceptVerbs("GET", "POST")]
        public IHttpActionResult GetBaoCaoDoanhThuSuaChuaTheoCoVan([FromBody] JObject objIn)
        {
            try
            {
                string IdChiNhanhs = "";
                if (objIn["IdChiNhanhs"] != null)
                {
                    List<string> lstChiNhanh = objIn["IdChiNhanhs"].ToObject<List<string>>();
                    if (lstChiNhanh.Count > 0)
                    {
                        IdChiNhanhs = string.Join(",", lstChiNhanh);
                    }
                }
                DateTime? ThoiGianFrom = null;
                if (objIn["ThoiGianFrom"] != null && objIn["ThoiGianFrom"].ToObject<string>() != "")
                    ThoiGianFrom = objIn["ThoiGianFrom"].ToObject<DateTime>();
                DateTime ThoiGianTo = DateTime.Now;
                if (objIn["ThoiGianTo"] != null && objIn["ThoiGianTo"].ToObject<string>() != "")
                    ThoiGianTo = objIn["ThoiGianTo"].ToObject<DateTime>();
                double? DoanhThuFrom = null;
                if (objIn["DoanhThuFrom"] != null && objIn["DoanhThuFrom"].ToObject<string>() != "")
                    DoanhThuFrom = objIn["DoanhThuFrom"].ToObject<double>();
                double? DoanhThuTo = null;
                if (objIn["DoanhThuTo"] != null && objIn["DoanhThuTo"].ToObject<string>() != "")
                    DoanhThuTo = objIn["DoanhThuTo"].ToObject<double>();
                double? LoiNhuanFrom = null;
                if (objIn["LoiNhuanFrom"] != null && objIn["LoiNhuanFrom"].ToObject<string>() != "")
                    LoiNhuanFrom = objIn["LoiNhuanFrom"].ToObject<double>();
                double? LoiNhuanTo = null;
                if (objIn["LoiNhuanTo"] != null && objIn["LoiNhuanTo"].ToObject<string>() != "")
                    LoiNhuanTo = objIn["LoiNhuanTo"].ToObject<double>();
                string TextSearch = "";
                if (objIn["TextSearch"] != null && objIn["TextSearch"].ToObject<string>() != "")
                    TextSearch = objIn["TextSearch"].ToObject<string>();
                double? SoLanTiepNhanFrom = null;
                if (objIn["SoLanTiepNhanFrom"] != null && objIn["SoLanTiepNhanFrom"].ToObject<string>() != "")
                    SoLanTiepNhanFrom = objIn["SoLanTiepNhanFrom"].ToObject<double>();
                double? SoLanTiepNhanTo = null;
                if (objIn["SoLanTiepNhanTo"] != null && objIn["SoLanTiepNhanTo"].ToObject<string>() != "")
                    SoLanTiepNhanTo = objIn["SoLanTiepNhanTo"].ToObject<double>();
                double? SoLuongHoaDonFrom = null;
                if (objIn["SoLuongHoaDonFrom"] != null && objIn["SoLuongHoaDonFrom"].ToObject<string>() != "")
                    SoLuongHoaDonFrom = objIn["SoLuongHoaDonFrom"].ToObject<double>();
                double? SoLuongHoaDonTo = null;
                if (objIn["SoLuongHoaDonTo"] != null && objIn["SoLuongHoaDonTo"].ToObject<string>() != "")
                    SoLuongHoaDonTo = objIn["SoLuongHoaDonTo"].ToObject<double>();
                string TrangThai = "";
                if (objIn["TrangThai"] != null && objIn["TrangThai"].ToObject<string>() != "")
                    TrangThai = objIn["TrangThai"].ToObject<string>();
                List<BaoCaoDoanhThuSuaChuaTheoCoVan> lstResult = new List<BaoCaoDoanhThuSuaChuaTheoCoVan>();
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    lstResult = reportGara.GetBaoCaoDoanhThuSuaChuaTheoCoVan(IdChiNhanhs, ThoiGianFrom, ThoiGianTo, SoLanTiepNhanFrom, SoLanTiepNhanTo, SoLuongHoaDonFrom, SoLuongHoaDonTo, DoanhThuFrom, DoanhThuTo,
                        LoiNhuanFrom, LoiNhuanTo, TextSearch, TrangThai);
                }
                return ActionTrueData(new
                {
                    data = lstResult
                });
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [HttpGet, HttpPost]
        public IHttpActionResult BaoCaoHoatDongXe_TongHop(ParamRpHoatDongXe param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<BaoCaoHoatDongXe_TongHop> data = reportGara.BaoCaoHoatDongXe_TongHop(param);
                    var lst = data.GroupBy(x =>
                    new
                    {
                        x.ID_DonVi,
                        x.ID_Xe,
                        x.ID_HangHoa,
                        x.TenDonVi,
                        x.TenNhomHangHoa,
                        x.MaHangHoa,
                        x.TenHangHoa,
                        x.BienSo,
                        x.SoGioHoatDong,
                        x.TongSoGioHoatDong,
                    })
                      .Select(x => new
                      {
                          x.Key.ID_DonVi,
                          x.Key.ID_Xe,
                          x.Key.ID_HangHoa,
                          x.Key.TenDonVi,
                          x.Key.TenNhomHangHoa,
                          x.Key.MaHangHoa,
                          x.Key.TenHangHoa,
                          x.Key.BienSo,
                          x.Key.SoGioHoatDong,
                          x.Key.TongSoGioHoatDong,
                          details = x,
                      });

                    int count = 0, page = 0;
                    int pageSize = param.PageSize ?? 10;
                    int pageNow = param.CurrentPage ?? 0;
                    if (data != null && data.Count() > 0)
                    {
                        count = data.FirstOrDefault().TotalRow ?? 0;
                    }
                    int[] listpage = GetListPage(count, pageSize, pageNow, ref page);

                    return ActionTrueData(new
                    {
                        data = lst,
                        listpage,
                        pageview = "Hiển thị " + (pageNow * pageSize + 1) + " - " + (pageNow * pageSize + data.Count()) + " trên tổng số " + count + " bản ghi",
                        isprev = pageNow > 3 && page > 5,
                        isnext = pageNow < page - 2 && page > 5,
                        countpage = page,
                        totalRow = count,
                    });
                }
                catch (Exception ex)
                {
                    return ActionFalseNotData(ex.Message);
                }
            }
        }
        [HttpGet, HttpPost]
        public IHttpActionResult BaoCaoHoatDongXe_ChiTiet(ParamRpHoatDongXe param)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                try
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<BaoCaoHoatDongXe_ChiTiet> data = reportGara.BaoCaoHoatDongXe_ChiTiet(param);
                    var lst = data.GroupBy(x => new
                    {
                        x.ID_PhieuTiepNhan,
                        x.ID_Xe,
                        x.ID_HangHoa,
                        x.TenDonVi,
                        x.MaPhieuTiepNhan,
                        x.NgayVaoXuong,
                        x.TenNhomHangHoa,
                        x.MaHangHoa,
                        x.TenHangHoa,
                        x.BienSo,
                        x.SoGioHoatDong,
                        x.TenNhanVien,
                        x.GhiChu,
                        x.TongSoGioHoatDong
                    })
                      .Select(x => new
                      {
                          x.Key.ID_PhieuTiepNhan,
                          x.Key.ID_Xe,
                          x.Key.ID_HangHoa,
                          x.Key.TenDonVi,
                          x.Key.MaPhieuTiepNhan,
                          x.Key.NgayVaoXuong,
                          x.Key.TenNhomHangHoa,
                          x.Key.MaHangHoa,
                          x.Key.TenHangHoa,
                          x.Key.BienSo,
                          x.Key.SoGioHoatDong,
                          x.Key.TenNhanVien,
                          x.Key.GhiChu,
                          x.Key.TongSoGioHoatDong,
                          details = x,
                      });

                    int count = 0, page = 0;
                    int pageSize = param.PageSize ?? 10;
                    int pageNow = param.CurrentPage ?? 0;
                    if (data != null && data.Count() > 0)
                    {
                        count = data.FirstOrDefault().TotalRow ?? 0;
                    }
                    int[] listpage = GetListPage(count, pageSize, pageNow, ref page);

                    return ActionTrueData(new
                    {
                        data = lst,
                        listpage,
                        pageview = "Hiển thị " + (pageNow * pageSize + 1) + " - " + (pageNow * pageSize + data.Count()) + " trên tổng số " + count + " bản ghi",
                        isprev = pageNow > 3 && page > 5,
                        isnext = pageNow < page - 2 && page > 5,
                        countpage = page,
                        totalRow = count,
                    });
                    return ActionTrueData(data);
                }
                catch (Exception ex)
                {
                    return ActionFalseNotData(ex.Message);
                }
            }
        }

        //[HttpPost]
        //public IHttpActionResult GetBaoCaoBaoDuongPhuTungTheoDoi([FromBody] JObject data)
        //{
        //    using (SsoftvnContext db = SystemDBContext.GetDBContext())
        //    {
        //        try
        //        {
        //            ClassReportGara reportGara = new ClassReportGara(db);
        //            return ActionTrueNotData("");
        //        }
        //        catch (Exception ex)
        //        {
        //            return ActionFalseNotData(ex.Message);
        //        }
        //    }
        //}

        [HttpPost]
        public IHttpActionResult GetListPhuTungTheoDoi([FromBody] JObject data)
        {
            try
            {
                GetListPhuTungTheoDoi_v1_Input lstInput = new GetListPhuTungTheoDoi_v1_Input();
                if (data["TextSearch"] != null)
                {
                    lstInput.TextSearch = data["TextSearch"].ToObject<string>();
                }
                if (data["CurrentPage"] != null)
                    lstInput.CurrentPage = data["CurrentPage"].ToObject<int>() - 1;
                if (data["PageSize"] != null)
                    lstInput.PageSize = data["PageSize"].ToObject<int>();
                if (data["TrangThais"] != null)
                {
                    lstInput.TrangThais = data["TrangThais"].ToObject<List<int>>();
                }
                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<GetListPhuTungTheoDoi_v1_Result> lstResult = reportGara.GetListPhuTungTheoDoi_v1(lstInput);
                    int count = 0;
                    if (lstResult.Count != 0)
                    {
                        count = lstResult[0].TotalRow;
                    }
                    int page = 0;
                    var listpage = GetListPage(count, lstInput.PageSize, lstInput.CurrentPage + 1, ref page);
                    return ActionTrueData(new
                    {
                        data = lstResult,
                        ListPage = listpage,
                        PageView = "Hiển thị " + ((lstInput.CurrentPage) * lstInput.PageSize + 1) + " - " + ((lstInput.CurrentPage) * lstInput.PageSize + lstResult.Count) + " trên tổng số " + count + " bản ghi",
                        NumberOfPage = page
                    });
                }
                return ActionTrueNotData("");
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        [HttpPost]
        public IHttpActionResult GetThoiGianHoatDong([FromBody] JObject data)
        {
            try
            {
                GetThoiGianHoatDong_v1_Input lstInput = new GetThoiGianHoatDong_v1_Input();
                if (data["TextSearch"] != null)
                {
                    lstInput.TextSearch = data["TextSearch"].ToObject<string>();
                }
                if (data["CurrentPage"] != null)
                    lstInput.CurrentPage = data["CurrentPage"].ToObject<int>() - 1;
                if (data["PageSize"] != null)
                    lstInput.PageSize = data["PageSize"].ToObject<int>();
                if (data["ThoiGianFrom"] != null && data["ThoiGianFrom"].ToObject<string>() != "")
                    lstInput.ThoiGianFrom = data["ThoiGianFrom"].ToObject<DateTime>();
                if (data["ThoiGianTo"] != null && data["ThoiGianTo"].ToObject<string>() != "")
                    lstInput.ThoiGianTo = data["ThoiGianTo"].ToObject<DateTime>();

                using (SsoftvnContext db = SystemDBContext.GetDBContext())
                {
                    ClassReportGara reportGara = new ClassReportGara(db);
                    List<GetThoiGianHoatDong_v1_Result> lstResult = reportGara.GetThoiGianHoatDong_v1(lstInput);
                    int count = 0;
                    if (lstResult.Count != 0)
                    {
                        count = lstResult[0].TotalRow;
                    }
                    int page = 0;
                    var listpage = GetListPage(count, lstInput.PageSize, lstInput.CurrentPage + 1, ref page);
                    return ActionTrueData(new
                    {
                        data = lstResult,
                        ListPage = listpage,
                        PageView = "Hiển thị " + ((lstInput.CurrentPage) * lstInput.PageSize + 1) + " - " + ((lstInput.CurrentPage) * lstInput.PageSize + lstResult.Count) + " trên tổng số " + count + " bản ghi",
                        NumberOfPage = page
                    });
                }

                return ActionTrueNotData("");
            }
            catch (Exception ex)
            {
                return ActionFalseNotData(ex.Message);
            }
        }

        #endregion

        public IHttpActionResult GetChecked(int? type, int? group)
        {
            var data = commonEnum.listNameReportTotal.ToList();
            switch (group)
            {
                case (int)commonEnum.GroupReport.banhang:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.chitiet:
                            data = commonEnum.listNameReportDetail.ToList();
                            break;
                        case (int)commonEnum.TypeReport.hangtralai:
                            data = commonEnum.listNameReportProductReturn.ToList();
                            break;
                        case (int)commonEnum.TypeReport.khachhang:
                            data = commonEnum.listNameReportCustomer.ToList();
                            break;
                        case (int)commonEnum.TypeReport.loinhuan:
                            data = commonEnum.listNameReportprofit.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhanvien:
                            data = commonEnum.listNameReportUser.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhomhang:
                            data = commonEnum.listNameReportGroupProduct.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhomkhachhang:
                            data = commonEnum.listNameReportGrCustomer.ToList();
                            break;
                        case (int)commonEnum.TypeReport.hangkhuyenmai:
                            data = commonEnum.DictionaryReportPromotion.ToList();
                            break;
                        case (int)commonEnum.TypeReport.khachhang_tansuat:
                            data = commonEnum.Dictionary_BaoCaoTanSuatDoanhThu.ToList();
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.nhaphang:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.ReportImportGoodsTotal.ToList();
                            break;
                        case (int)commonEnum.TypeReport.chitiet:
                            data = commonEnum.ReportImportGoodsDetail.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhomhang:
                            data = commonEnum.ReportImportGoodsGroup.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhacungcap:
                            data = commonEnum.ReportImportGoodsSupplier.ToList();
                            break;
                        case (int)commonEnum.TypeReport.trahangnhap:
                            data = commonEnum.ReportImportGoodsReturn.ToList();
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.taichinh:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.ReportFinancialTotal.ToList();
                            break;
                        case (int)commonEnum.TypeReport.thutien:
                            data = commonEnum.ReportFinancialCollectMoney.ToList();
                            break;
                        case (int)commonEnum.TypeReport.chitien:
                            data = commonEnum.ReportFinancialPay.ToList();
                            break;
                        case (int)commonEnum.TypeReport.soquy:
                            data = commonEnum.ReportFinancialBookCash.ToList();
                            break;
                        case (int)commonEnum.TypeReport.soquy2:
                            data = commonEnum.ReportFinancialBookBank.ToList();
                            break;
                        case (int)commonEnum.TypeReport.soquy3:
                            data = commonEnum.ReportFinancialSurvive.ToList();
                            break;
                        default:
                            data = null;
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.dathang:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.ReportOrderTotal.ToList();
                            break;
                        case (int)commonEnum.TypeReport.chitiet:
                            data = commonEnum.ReportOrderDetail.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhomhang:
                            data = commonEnum.ReportOrderGroup.ToList();
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.kho:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.ReportWarehouseTotal.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhapxuatton:
                            data = commonEnum.ReportWarehouseExport.ToList();
                            break;
                        case (int)commonEnum.TypeReport.xuattonchitiet:
                            data = commonEnum.ReportWarehouseDetail.ToList();
                            break;
                        case (int)commonEnum.TypeReport.dieuchuyenhhXC:
                            data = commonEnum.ReportWarehouseTransportExport.ToList();
                            break;
                        case (int)commonEnum.TypeReport.dieuchuyenhhNC:
                            data = commonEnum.ReportWarehouseTransportImport.ToList();
                            break;
                        case (int)commonEnum.TypeReport.dieuchuyenhhCT:
                            data = commonEnum.ReportWarehouseTransportDetail.ToList();
                            break;
                        case (int)commonEnum.TypeReport.thnhapkhoHH:
                        case (int)commonEnum.TypeReport.thnhapkhopthong:
                            data = commonEnum.ReportWarehouseImportStoreHH.ToList();
                            break;
                        case (int)commonEnum.TypeReport.thnhapkhoGD:
                        case (int)commonEnum.TypeReport.thnhapkhopthongGD:
                            data = commonEnum.BaoCaoNhapKhoChiTiet.ToList();
                            break;
                        case (int)commonEnum.TypeReport.thxuatkhoHH:
                            data = commonEnum.ReportWarehouseExportStoreHH.ToList();
                            break;
                        case (int)commonEnum.TypeReport.thxuatkhoGD:
                            data = commonEnum.ReportWarehouseExportStoreGD.ToList();
                            break;
                        case (int)commonEnum.TypeReport.xuatkhohhtheodinhluong:
                            data = commonEnum.ListReportDinhLuong.ToList();
                            break;
                        case (int)commonEnum.TypeReport.tonkhoth:
                            data = null;
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.goidichvu:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.TypeRGoiDichVuDuTH.ToList();
                            break;
                        case (int)commonEnum.TypeReport.duchitiet:
                            data = commonEnum.TypeRGoiDichVuDuCT.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhatkysdth:
                            data = commonEnum.TypeRGoiDichVuNhatKyTH.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhatkysdct:
                            data = commonEnum.TypeRGoiDichVuNhatKyCT.ToList();
                            break;
                        case (int)commonEnum.TypeReport.tonchuasudung:
                            data = commonEnum.TypeRGoiDichVuTonChuaSD.ToList();
                            break;
                        case (int)commonEnum.TypeReport.nhapxuatton:
                            data = commonEnum.TypeRGoiDichVuNhatXuatTon.ToList();
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.chietkhau:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.tonghop:
                            data = commonEnum.TypeRChietKhauTH.ToList();
                            break;
                        case (int)commonEnum.TypeReport.chitiet:
                            data = commonEnum.TypeRChietKhauCT.ToList();
                            break;
                        default:
                            break;
                    }
                    break;
                case (int)commonEnum.GroupReport.nhanvien:
                    switch (type)
                    {
                        case (int)commonEnum.TypeReport.rnv_th:
                            data = commonEnum.ListReportNVTongHop.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_baohiem:
                            data = commonEnum.ListReportNVBaoHiem.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_congtac:
                            data = commonEnum.ListReportNVCongTac.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_daotao:
                            data = commonEnum.ListReportNVDaoTao.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_giadinh:
                            data = commonEnum.ListReportNVGiaDinh.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_hopdong:
                            data = commonEnum.ListReportNVHopDong.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_kyluat:
                            data = commonEnum.ListReportNVKyluat.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_miengiamthue:
                            data = commonEnum.ListReportNVThue.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_phucap:
                            data = commonEnum.ListReportNVPhuCap.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_suckhoe:
                            data = commonEnum.ListReportNVSucKhoe.ToList();
                            break;
                        case (int)commonEnum.TypeReport.rnv_tuoi:
                            data = commonEnum.ListReportNVTuoi.ToList();
                            break;
                        default:
                            data = commonEnum.ListReportNVTongHop.ToList();
                            break;
                    }
                    break;

            }

            return Json(data);
        }
    }

    public class JsonResultExample<T>
    {
        public int Rowcount { get; set; }
        public List<T> LstData { get; set; }
        public List<T> LstDataPrint { get; set; }
        public List<ListLHPages> LstPageNumber { get; set; }
        public double _thanhtien { get; set; }
        public double _tienvon { get; set; }
        public double _lailo { get; set; }
        public double _soluong { get; set; }
    }

    public class JsonResultExampleCongViec<T>
    {
        public List<T> LstData { get; set; }
        public double PageCount { get; set; }
        public double TotalRow { get; set; }
    }

    public class JsonResultExampleTr<T>
    {
        public int Rowcount { get; set; }
        public List<T> LstData { get; set; }
        public List<ListLHPages> LstPageNumber { get; set; }
        public int numberPage { get; set; }
        public double a1 { get; set; }
        public double a2 { get; set; }
        public double a3 { get; set; }
        public double a4 { get; set; }
        public double a5 { get; set; }
        public double a6 { get; set; }
        public double a7 { get; set; }
        public double a8 { get; set; }
        public double a9 { get; set; }
        public double a10 { get; set; }
        public double a11 { get; set; }
        public double a12 { get; set; }
        public double a13 { get; set; }
        public double a14 { get; set; }
        public double a15 { get; set; }
    }
    public class JsonResultExample_HangHoa<T>
    {
        public int Rowcount { get; set; }
        public List<T> LstData { get; set; }
        public List<T> LstDataPrint { get; set; }
        public List<ListLHPages> LstPageNumber { get; set; }
        public double SoLuongBan { get; set; }
        public double GiaTriBan { get; set; }
        public double SoLuongTra { get; set; }
        public double GiaTriTra { get; set; }
        public double DoanhThuThuan { get; set; }
        public double TongGiaVon { get; set; }
        public double LoiNhuan { get; set; }
        public double TySuat { get; set; }
    }
    public class JsonResultExample_BieuDo<T>
    {
        public List<T> LstData { get; set; }
        public List<T> LstDate { get; set; }
        public List<T> LstChiNhanh { get; set; }
        public double TongTien { get; set; }
    }
    public class JsonResult_BieuDo<T>
    {
        public List<T> Lst_BieuDo1 { get; set; }
        public List<T> Lst_BieuDo2 { get; set; }
    }
    public class JsonResultExample_LoiNhuan<T>
    {
        public int Rowcount { get; set; }
        public List<T> LstData { get; set; }
        public List<T> LstDataPrint { get; set; }
        public List<ListLHPages> LstPageNumber { get; set; }
        public double TongTienHang { get; set; }
        public double GiamGiaHD { get; set; }
        public double DoanhThu { get; set; }
        public double GiaTriTra { get; set; }
        public double DoanhThuThuan { get; set; }
        public double TongGiaVon { get; set; }
        public double LoiNhuanGop { get; set; }
    }
    public class JsonResultExample_XNT<T>
    {
        public int Rowcount { get; set; }
        public List<T> LstData { get; set; }
        public List<T> LstDataPrint { get; set; }
        public List<ListLHPages> LstPageNumber { get; set; }
        public double TonDauKy { get; set; }
        public double GiaTriDauKy { get; set; }
        public double SoLuongNhap_NCC { get; set; }
        public double SoLuongNhap_Kiem { get; set; }
        public double SoLuongNhap_Tra { get; set; }
        public double SoLuongNhap_Chuyen { get; set; }
        public double SoLuongNhap_SX { get; set; }
        public double SoLuongXuat_Ban { get; set; }
        public double SoLuongXuat_Huy { get; set; }
        public double SoLuongXuat_NCC { get; set; }
        public double SoLuongXuat_Kiem { get; set; }
        public double SoLuongXuat_Chuyen { get; set; }
        public double SoLuongXuat_SX { get; set; }
        public double TonCuoiKy { get; set; }
        public double GiaTriCuoiKy { get; set; }
    }
}


