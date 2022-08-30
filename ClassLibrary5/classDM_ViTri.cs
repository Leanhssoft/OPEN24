using Model;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Validation;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace libDM_ViTri
{
    class classDM_ViTri
    {
        #region select
        public static DM_ViTri Select_PhongBan(Guid id)
        {
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return null;
            }
            else
            {
                return db.DM_ViTri.Find(id);
            }
        }

        public static IQueryable<DM_ViTri> Gets(Expression<Func<DM_ViTri, bool>> query)
        {
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return null;
            }
            else
            {
                if (query == null)
                    return db.DM_ViTri;
                else
                    return db.DM_ViTri.Where(query);
            }
        }
        public static DM_ViTri Get(Expression<Func<DM_ViTri, bool>> query)
        {
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return null;
            }
            else
            {
                return db.DM_ViTri.Where(query).FirstOrDefault();
            }
        }

        public static bool DM_ViTriExists(Guid id)
        {
            SsoftvnContext db = SystemDBContext.GetDBContext();

            if (db == null)
            {
                return false;
            }
            else
            {

                return db.DM_ViTri.Count(e => e.ID == id) > 0;
            }
        }

        #endregion

        #region insert
        public static string Add_ViTri(DM_ViTri objAdd)
        {
            string strErr = string.Empty;
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return "Kết nối CSDL không hợp lệ";
            }
            else
            {
                try
                {
                    db.DM_ViTri.Add(objAdd);
                    db.SaveChanges();
                }
                catch (DbEntityValidationException dbEx)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var eve in dbEx.EntityValidationErrors)
                    {
                        sb.AppendLine(string.Format("Entity of type \"{0}\" in state \"{1}\" has the following validation errors:",
                                                        eve.Entry.Entity.GetType().Name,
                                                        eve.Entry.State));
                        foreach (var ve in eve.ValidationErrors)
                        {
                            sb.AppendLine(string.Format("- Property: \"{0}\", Error: \"{1}\"",
                                                        ve.PropertyName,
                                                        ve.ErrorMessage));
                        }
                    }
                    throw new DbEntityValidationException(sb.ToString(), dbEx);
                }
            }
            return strErr;
        }
        #endregion

        #region update
        public static string Update_ViTri(DM_ViTri obj)
        {
            string strErr = string.Empty;
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return "Kết nối CSDL không hợp lệ";
            }
            else
            {
                try
                {
                    #region DM_ViTri
                    DM_ViTri objUpd = db.DM_ViTri.Find(obj.ID);
                    objUpd.ID = obj.ID;
                    objUpd.TenViTri = obj.TenViTri;
                    objUpd.GhiChu = obj.GhiChu;

                    #endregion


                    db.Entry(objUpd).State = EntityState.Modified;
                    //
                    db.SaveChanges();

                }
                catch (Exception ex)
                {
                    strErr = ex.Message;
                }
            }
            return strErr;
        }
        #endregion

        #region delete
        static string CheckDelete_ViTri(SsoftvnContext db, DM_ViTri obj)
        {
            string strCheck = string.Empty;

            List<CongDoan_DichVu> lstCongDoans = db.CongDoan_DichVu.Where(p => p.ID_CongDoan == obj.ID).ToList();
            if (lstCongDoans != null && lstCongDoans.Count > 0)
            {
                strCheck = "Hàng hóa/Dịch vụ đã được sử dụng để lập danh mục công đoạn cho hàng hóa/dịch vụ khác.";
                return strCheck;
            }

            return strCheck;
        }

        public static string Delete_ViTri(Guid id)
        {
            string strErr = string.Empty;
            SsoftvnContext db = SystemDBContext.GetDBContext();
            if (db == null)
            {
                return "Kết nối CSDL không hợp lệ";
            }
            else
            {
                DM_ViTri objDel = db.DM_ViTri.Find(id);
                if (objDel != null)
                {
                    string strCheck = CheckDelete_ViTri(db, objDel);
                    if (strCheck == string.Empty)
                    {
                        try
                        {
                            List<CongDoan_DichVu> lstCongDoans = db.CongDoan_DichVu.Where(p => p.ID_DichVu == id).ToList();
                            if (lstCongDoans != null && lstCongDoans.Count > 0)
                            {
                                db.CongDoan_DichVu.RemoveRange(lstCongDoans);
                            }
                            //
                            db.DM_ViTri.Remove(objDel);
                            //
                            db.SaveChanges();
                        }
                        catch (Exception exxx)
                        {
                            strErr = exxx.Message;
                            return strErr;
                        }
                    }
                    else
                    {
                        strErr = strCheck;
                        return strErr;
                    }
                }
                else
                {
                    strErr = "Không tìm thấy dữ liệu cần xử lý trên hệ thống.";
                    return strErr;
                }
            }
            return strErr;
        }
        #endregion

        #region
        public static string GetautoCode(int loaiViTri)
        {
            string format = "{0:0000}";
            SsoftvnContext db = SystemDBContext.GetDBContext();
            string autoCode = string.Empty;

            switch (loaiViTri)
            {
                case 1:
                    autoCode = "VT0";
                    break;
                case 2:
                    autoCode = "NCC0";
                    break;
                default:
                    autoCode = "KH_NCC0";
                    break;
            }
            string sCode = db.DM_ViTri.Where(p => p.MaViTri.Contains(autoCode)).Where(p => p.MaViTri.Length == 7).OrderByDescending(p => p.MaViTri).Select(p => p.MaViTri).FirstOrDefault();
            if (sCode == null)
            {
                autoCode = autoCode + string.Format(format, 1);
            }
            else
            {
                int tempstt = int.Parse(sCode.Substring(autoCode.Length, 4)) + 1;
                autoCode = autoCode + string.Format(format, tempstt);
            }
            return autoCode;
        }

        #endregion
    }
}
