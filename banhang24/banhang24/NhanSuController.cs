using banhang24.Hellper;
using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web.Mvc;
using banhang24.Models;
using libNS_NhanVien;

namespace banhang24.Controllers
{
    public class NhanSuController : Controller
    {
        [RBACAuthorize(RoleKey = RoleNhanSu.CaLamViec_XemDS)]
        public ActionResult CaLamViec()
        {
            try
            {
                using (SsoftvnContext _dbcontext = SystemDBContext.GetDBContext())
                {
                    var ID_ND = new Guid(CookieStore.GetCookieAes(SystemConsts.NGUOIDUNGID));
                    var RoleModel = new RoleModel() { Delete = true, Export = true, Import = true, Insert = true, Update = true, View = true };
                    if (!_dbcontext.HT_NguoiDung.Any(o => o.ID == ID_ND && o.LaAdmin))
                    {
                        var _NhanSuService = new NhanSuService(_dbcontext);
                        var listQuyen = _NhanSuService.GetListQuyen(ID_ND).Select(o => o.MaQuyen);
                        RoleModel.Insert = listQuyen.Any(o => o.Equals(RoleNhanSu.CaLamViec_ThemMoi));
                        RoleModel.Update = listQuyen.Any(o => o.Equals(RoleNhanSu.CaLamViec_CapNhat));
                        RoleModel.Delete = listQuyen.Any(o => o.Equals(RoleNhanSu.CaLamViec_Xoa));
                        RoleModel.Export = listQuyen.Any(o => o.Equals(RoleNhanSu.CaLamViec_XuatFile));
                        RoleModel.Import = listQuyen.Any(o => o.Equals(RoleNhanSu.CaLamViec_NhapFile));
                    }
                    return View(RoleModel);
                }
            }
            catch(Exception ex)
            {
                return View(new RoleModel() { Delete = false, Export = false, Import = false, Insert = false, Update = false, Log = ex.Message });
            }
        }

        [RBACAuthorize(RoleKey = RoleNhanSu.PhieuPhanCa_XemDS)]
        public ActionResult PhanCongCaLamViec()
        {
            try
            {
                using (SsoftvnContext _dbcontext = SystemDBContext.GetDBContext())
                {
                    var ID_ND = new Guid(CookieStore.GetCookieAes(SystemConsts.NGUOIDUNGID));
                    var RoleModel = new RoleModel() { Delete = true, Insert = true, Update = true,Export=true };
                    if (!_dbcontext.HT_NguoiDung.Any(o => o.ID == ID_ND && o.LaAdmin))
                    {
                        var _NhanSuService = new NhanSuService(_dbcontext);
                        var listQuyen = _NhanSuService.GetListQuyen(ID_ND).Select(o => o.MaQuyen);
                        RoleModel.Insert = listQuyen.Any(o => o.Equals(RoleNhanSu.PhieuPhanCa_ThemMoi));
                        RoleModel.Update = listQuyen.Any(o => o.Equals(RoleNhanSu.PhieuPhanCa_CapNhat));
                        RoleModel.Delete = listQuyen.Any(o => o.Equals(RoleNhanSu.PhieuPhanCa_Xoa));
                        RoleModel.Export = listQuyen.Any(o => o.Equals(RoleNhanSu.PhieuPhanCa_XuatFile));
                    }
                    return View(RoleModel);
                }
            }
            catch (Exception ex)
            {
                return View(new RoleModel() { Delete = false, Insert = false, Update = false, Export = false, Log = ex.Message });
            }
            return View();
        }

        [RBACAuthorize(RoleKey = RoleNhanSu.KyTinhCong_XemDS)]
        public ActionResult KyTinhCong()
        {

            try
            {
                using (SsoftvnContext _dbcontext = SystemDBContext.GetDBContext())
                {
                    var ID_ND = new Guid(CookieStore.GetCookieAes(SystemConsts.NGUOIDUNGID));
                    var RoleModel = new RoleKyTinhCongModel() { Delete = true, Insert = true, Update = true, ChotCong = true };
                    if (!_dbcontext.HT_NguoiDung.Any(o => o.ID == ID_ND && o.LaAdmin))
                    {
                        var _NhanSuService = new NhanSuService(_dbcontext);
                        var listQuyen = _NhanSuService.GetListQuyen(ID_ND).Select(o => o.MaQuyen);
                        RoleModel.Insert = listQuyen.Any(o => o.Equals(RoleNhanSu.KyTinhCong_ThemMoi));
                        RoleModel.Update = listQuyen.Any(o => o.Equals(RoleNhanSu.KyTinhCong_CapNhat));
                        RoleModel.Delete = listQuyen.Any(o => o.Equals(RoleNhanSu.KyTinhCong_Xoa));
                        RoleModel.ChotCong = listQuyen.Any(o => o.Equals(RoleNhanSu.KyTinhCong_ChotCong));
                    }
                    return View(RoleModel);
                }
            }
            catch (Exception ex)
            {
                return View(new RoleKyTinhCongModel() { Delete = false, Insert = false, Update = false, ChotCong = false, Log = ex.Message });
            }
        }

        public ActionResult KyHieuCong()
        {
            return View();
        }

        [RBACAuthorize(RoleKey = RoleNhanSu.ChamCong_XemDS)]
        public ActionResult ChamCong()
        {
            try
            {
                using (SsoftvnContext _dbcontext = SystemDBContext.GetDBContext())
                {
                    var ID_ND = new Guid(CookieStore.GetCookieAes(SystemConsts.NGUOIDUNGID));
                    var RoleModel = new RoleChamCongModel() { Export = true, Insert = true, GuiBangCong = true , ChamCong = true};
                    if (!_dbcontext.HT_NguoiDung.Any(o => o.ID == ID_ND && o.LaAdmin))
                    {
                        var _NhanSuService = new NhanSuService(_dbcontext);
                        var listQuyen = _NhanSuService.GetListQuyen(ID_ND).Select(o => o.MaQuyen);
                        RoleModel.Insert = listQuyen.Any(o => o.Equals(RoleNhanSu.ChamCong_AddHoSo));
                        RoleModel.ChamCong = listQuyen.Any(o => o.Equals(RoleNhanSu.ChamCong_ChamCong));
                        RoleModel.Export = listQuyen.Any(o => o.Equals(RoleNhanSu.ChamCong_XuatFile));
                        RoleModel.GuiBangCong = listQuyen.Any(o => o.Equals(RoleNhanSu.ChamCong_GuiBangCong));
                    }
                    return View(RoleModel);
                }
            }
            catch (Exception ex)
            {
                return View(new RoleChamCongModel() { Export = false, Insert = false, GuiBangCong = false,ChamCong=false, Log = ex.Message });
            }
        }

        [RBACAuthorize(RoleKey = RoleNhanSu.BangLuong_PheDuyet)]
        public ActionResult BangLuong()
        {
            try
            {
                using (SsoftvnContext _dbcontext = SystemDBContext.GetDBContext())
                {
                    var ID_ND = new Guid(CookieStore.GetCookieAes(SystemConsts.NGUOIDUNGID));
                    var RoleModel = new RoleKyTinhCongModel() {  Insert = true, Update = true,Export=true, ChotCong = true };
                    if (!_dbcontext.HT_NguoiDung.Any(o => o.ID == ID_ND && o.LaAdmin))
                    {
                        var _NhanSuService = new NhanSuService(_dbcontext);
                        var listQuyen = _NhanSuService.GetListQuyen(ID_ND).Select(o => o.MaQuyen);
                        RoleModel.Insert = listQuyen.Any(o => o.Equals(RoleNhanSu.BangLuong_ThemMoi));
                        RoleModel.Update = listQuyen.Any(o => o.Equals(RoleNhanSu.BangLuong_CapNhat));
                        RoleModel.ChotCong = listQuyen.Any(o => o.Equals(RoleNhanSu.BangLuong_PheDuyet));
                    }
                    return View(RoleModel);
                }
            }
            catch (Exception ex)
            {
                return View(new RoleKyTinhCongModel() { Export = false, Insert = false, Update = false, ChotCong = false, Log = ex.Message });
            }
        }

        public ActionResult LoaiBaoHiem()
        {
            return View();
        }
        public ActionResult LoaiKhenThuong()
        {
            return View();
        }
    }
}
