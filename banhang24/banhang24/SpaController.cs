using libHT_NguoiDung;
using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Services;
using System.Web.UI;

namespace banhang24.Controllers
{
    [App_Start.App_API.CheckwebAuthorize]
    public class SpaController : Controller
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
        public ActionResult _addNhanVienTH()
        {
            return PartialView();
        }
        public ActionResult _addNVTuVan()
        {
            return PartialView();
        }

        public ActionResult Spa()
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

        //[WebMethod]
        //public ActionResult PasData(Array[] arr)
        //{
        //    return RedirectToAction("Index");
        //}
    }
}
