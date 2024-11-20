using Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace libHT_NguoiDung
{
    public class classHT_Quyen
    {
        private SsoftvnContext db;
        public classHT_Quyen(SsoftvnContext _db)
        {
            db = _db;
        }
        public IQueryable<HT_Quyen> Gets(Expression<Func<HT_Quyen, bool>> query)
        {
            if (db == null)
            {
                return null;
            }
            else
            {
                if (query == null)
                    return db.HT_Quyen;
                else
                    return db.HT_Quyen.Where(query);
            }
        }
        public string Insert(HT_Quyen newQuyen)
        {
            if (db == null)
            {
                return "Chưa kết nối DB";
            }
            try
            {
                db.HT_Quyen.Add(newQuyen);
                db.SaveChanges();
                return "Thêm quyền thành công";
            }
            catch (Exception ex)
            {
                return $"Lỗi khi thêm quyền: {ex.Message}";
            }
        }

        public string Update(HT_Quyen updatedQuyen)
        {
            if (db == null)
            {
                return "Chưa kết nối DB";
            }
            try
            {
                var existingQuyen = db.HT_Quyen.Find(updatedQuyen.MaQuyen);
                if (existingQuyen == null)
                {
                    return "Quyền không tồn tại";
                }

                existingQuyen.TenQuyen = updatedQuyen.TenQuyen;
                existingQuyen.QuyenCha = updatedQuyen.QuyenCha;
                existingQuyen.DuocSuDung = updatedQuyen.DuocSuDung;

                db.SaveChanges();
                return "Cập nhật quyền thành công";
            }
            catch (Exception ex)
            {
                return $"Lỗi khi cập nhật quyền: {ex.Message}";
            }
        }

        public string Delete(string maQuyen)
        {
            if (db == null)
            {
                return "Chưa kết nối DB";
            }
            try
            {
                var quyenToDelete = db.HT_Quyen.Find(maQuyen);
                if (quyenToDelete == null)
                {
                    return "Quyền không tồn tại";
                }

                db.HT_Quyen.Remove(quyenToDelete);
                db.SaveChanges();
                return "Xóa quyền thành công";
            }
            catch (Exception ex)
            {
                return $"Lỗi khi xóa quyền: {ex.Message}";
            }
        }

    }
}
