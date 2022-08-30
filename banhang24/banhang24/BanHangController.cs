using libHT_NguoiDung;
using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Mvc;
using banhang24.Hellper;

namespace banhang24.Controllers
{
    [App_Start.App_API.CheckwebAuthorize]
    public class BanHangController : Controller
    {
        //[OutputCache(Location = OutputCacheLocation.ServerAndClient, Duration = int.MaxValue)]
        public ActionResult Index()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }
        public ActionResult _thanhToan()
        {
            return PartialView();
        }
        public ActionResult NhaBep()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                classHT_NguoiDung_Nhom classHTNguoiDungNhom = new classHT_NguoiDung_Nhom(db);

                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);

                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.cookieIDUser = objUser_Cookies.ID;
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;

                    var ID_ND = new Guid(CookieStore.GetCookieAes("id_nguoidung"));
                    List<string> lstMaquyen = classHTNguoiDung.Select_HT_Quyen_Nhom(classHTNguoiDungNhom.Gets(p => p.IDNguoiDung == ID_ND).FirstOrDefault().IDNhomNguoiDung)
                                            .Select(p => p.MaQuyen).ToList();
                    string permision = String.Join(",", lstMaquyen.ToArray());
                    if (permision.Contains("NhaBep_TruyCap"))
                    {
                        return View();
                    }
                    else
                    {
                        return RedirectToAction("Index", "Home");
                    }
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult BanHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                banhang24.AppCache.EventUpdateCache.CreatFIleAppcache();
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult SalesOrder()
        {
            return View();
        }

        public ActionResult _getHoaDonOffline()
        {
            return PartialView();
        }

        public ActionResult HoaDon()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.cookieIDUser = objUser_Cookies.ID;
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;
                    ViewBag.cookieIDNhanVien = objUser_Cookies.ID_NhanVien;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.NhaBep_TruyCap)]
        public ActionResult Kitchen()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.cookieIDUser = objUser_Cookies.ID;
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult _thanhToanThe()
        {
            return PartialView();
        }

        public ActionResult BanLe_TraHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult DisplayCustomer()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult POS_Display()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult POSDisplay(string id)
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                var subDomain = CookieStore.GetCookieAes("SubDomain");
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieDonVi = objUser_Cookies.ID_DonVi;
                    ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                    ViewBag.SubDomain = subDomain;
                    ViewBag.imgaddr = id == null? "" : id;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult BanLe()
        {
            return View();
        }
        public ActionResult _PrintHoaDon()
        {
            return PartialView();
        }

        public ActionResult _trahanghoadon()
        {
            return PartialView();
        }

        public ActionResult _xuliDatHang()
        {
            return PartialView();
        }

        public ActionResult _traTienKhach()
        {
            return PartialView();
        }

        public ActionResult _lapPhieuThu()
        {
            return PartialView();
        }

        public ActionResult _thanhToanThePhieuThu()
        {
            return PartialView();
        }
        public ActionResult _CaculatorPrice()
        {
            return PartialView();
        }

        public ActionResult _CaculatorQuantity()
        {
            return PartialView();
        }

        public ActionResult _ThanhToanThe_NewInterface()
        {
            return PartialView();
        }

        public ActionResult _CreateAccountBank()
        {
            return PartialView();
        }

        public ActionResult _ThietLapMauIn()
        {
            return PartialView();
        }

        public ActionResult _SettingHideShowColumnCTHD()
        {
            return PartialView();
        }

        public ActionResult _ThanhToanThePhieuThu_New()
        {
            return PartialView();
        }
    }
}
