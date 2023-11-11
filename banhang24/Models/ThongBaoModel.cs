using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace banhang24.Models
{
    public class ThongBaoModel
    {
    }
    public class HT_ThongBaoDTO
    {
        public bool DaDoc { get; set; }
        public string NoiDungThongBao { get; set; }
        public string NgayTao { get; set; }
        public string Image { get; set; }
        public Guid? ID { get; set; }
        public int? LoaiThongBao { get; set; }
        public DateTime? ThoiGian { get; set; }
        public string NguoiDungDaDoc { get; set; }
    }

}