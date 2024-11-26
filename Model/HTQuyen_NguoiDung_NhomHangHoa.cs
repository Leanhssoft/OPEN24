using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Model
{
    public class HTQuyen_NguoiDung_NhomHangHoa
    {
        [Key]
        [Column(TypeName = "uniqueidentifier")]
        public Guid ID { get; set; }
        [Column(TypeName = "uniqueidentifier")]
        public Guid ID_NguoiDung { get; set; }
        [ForeignKey(nameof(ID_NguoiDung))]
        public HT_NguoiDung HT_NguoiDung { get; set; }

        [Column(TypeName = "uniqueidentifier")]
        public Guid ID_NhomHang { get; set; }
        [ForeignKey(nameof(ID_NhomHang))]
        public DM_NhomHangHoa DM_NhomHangHoa { get; set; }
    }
}
