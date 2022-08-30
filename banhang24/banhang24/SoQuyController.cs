using banhang24.Hellper;
using libHT_NguoiDung;
using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Mvc;

namespace banhang24.Controllers
{
    [App_Start.App_API.CheckwebAuthorize]
    public class SoQuyController : Controller
    {
        [RBACAuthorize(RoleKey = RoleKey.SoQuy_XemDs)]
        public ActionResult Index()
        {
            string apiUrl = Url.HttpRouteUrl("DefaultApi", new { controller = "SoQuy" });
            ViewBag.LoaiHoaDon = null;
            ViewBag.ApiUrl = new Uri(Request.Url, apiUrl).AbsoluteUri.ToString();
            return View();
        }
        [RBACAuthorize(RoleKey = RoleKey.SoQuy_XemDs)]
        public ActionResult SoQuy()
        {
            string apiUrl = Url.HttpRouteUrl("DefaultApi", new { controller = "SoQuy" });
            ViewBag.LoaiHoaDon = null;
            ViewBag.ApiUrl = new Uri(Request.Url, apiUrl).AbsoluteUri.ToString();
            return View();
        }

        public ActionResult _themmoiphieuthu()
        {
            ViewBag.LoaiHoaDon = 11;
            return PartialView();
        }

        public ActionResult _themmoiphieuchi()
        {
            ViewBag.LoaiHoaDon = 12;
            return PartialView();
        }

        public ActionResult _themmoiloaithu()
        {
            return PartialView();
        }

        public ActionResult _themmoiloaichi()
        {
            return PartialView();
        }

        public ActionResult _editphieuthuHD()
        {
            return PartialView();
        }

        public ActionResult _editphieuchiHD()
        {
            return PartialView();
        }

        public ActionResult _themmoiphieuthunganhang()
        {
            return PartialView();
        }

        public ActionResult _themmoiphieuchinganhang()
        {
            return PartialView();
        }
    }
}
