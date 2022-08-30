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
using libDM_LoaiChungTu;

namespace banhang24.Controllers
{
    public class GiaoDichController : Controller
    {
        [App_Start.App_API.CheckwebAuthorize]

        [RBACAuthorize(RoleKey = RoleKey.HoaDon_XemDs)]
        public ActionResult HoaDon()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                // get cookie nganh nghe kinh doanh
                ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;

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

        [RBACAuthorize(RoleKey = RoleKey.TraHang_XemDs)]
        public ActionResult TraHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();

                if (objUser_Cookies != null)
                {
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;

                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.NhapHang_XemDs)]
        public ActionResult NhapHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    string apiUrl = Url.HttpRouteUrl("DefaultApi", new { controller = "GiaoDich" });
                    ViewBag.ApiUrl = new Uri(Request.Url, apiUrl).AbsoluteUri.ToString();
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }


        public ActionResult NhapHangSanXuat()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    string apiUrl = Url.HttpRouteUrl("DefaultApi", new { controller = "GiaoDich" });
                    ViewBag.ApiUrl = new Uri(Request.Url, apiUrl).AbsoluteUri.ToString();
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.TraHangNhap_XemDs)]
        public ActionResult TraHangNhap()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    string apiUrl = Url.HttpRouteUrl("DefaultApi", new { controller = "GiaoDich" });
                    ViewBag.ApiUrl = new Uri(Request.Url, apiUrl).AbsoluteUri.ToString();

                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.NhapHang_XemDs)]
        public ActionResult NhapHangItem()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.LoaiHoaDon = 4;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.NhapHang_XemDs)]
        public ActionResult NhapHangItem1_2()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.LoaiHoaDon = 4;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult _ThemMoiHangHoa()
        {
            return PartialView();
        }

        public ActionResult _ShowModalMessage()
        {
            return PartialView();
        }

        [RBACAuthorize(RoleKey = RoleKey.TraHangNhap_XemDs)]
        public ActionResult TraHangNhapChiTiet()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.LoaiHoaDon = 7;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult _modalDelete()
        {
            return PartialView();
        }

        [RBACAuthorize(RoleKey = RoleKey.ChuyenHang_XemDs)]
        public ActionResult ChuyenHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.LoaiHoaDon = 10;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.ChuyenHang_XemDs)]
        public ActionResult ChuyenHangChiTiet()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                if (objUser_Cookies != null)
                {
                    ViewBag.LoaiHoaDon = 10;
                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.XuatHuy_XemDs)]
        public ActionResult XuatHuy()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
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

        public ActionResult PhieuXuatHuy()
        {
            return View();
        }

        public ActionResult DonHangTra()
        {
            return View();
        }

        [RBACAuthorize(RoleKey = RoleKey.DatHang_XemDs)]
        public ActionResult DatHang()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;

                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        [RBACAuthorize(RoleKey = RoleKey.GoiDichVu_XemDs)]
        public ActionResult GoiDichVu()
        {
            using (SsoftvnContext db = SystemDBContext.GetDBContext())
            {
                classHT_NguoiDung classHTNguoiDung = new classHT_NguoiDung(db);
                userLogin objUser_Cookies = classHTNguoiDung.GetUserCookies(this);
                ViewBag.ShopCookies = CookieStore.GetCookieAes("shop").ToUpper();
                if (objUser_Cookies != null)
                {
                    ViewBag.cookieUserLogin = objUser_Cookies.TaiKhoan;

                    return View();
                }
                else
                {
                    return RedirectToAction("Login", "Home");
                }
            }
        }

        public ActionResult _lapPhieuThuHoaDon()
        {
            return PartialView();
        }

        public ActionResult _popupbanggiaban()
        {
            return PartialView();
        }

        public ActionResult NapTienTheGiaTri()
        {
            return View();
        }
    }
}
