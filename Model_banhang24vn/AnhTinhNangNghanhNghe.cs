//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Model_banhang24vn
{
    using System;
    using System.Collections.Generic;
    
    public partial class AnhTinhNangNghanhNghe
    {
        public long Id { get; set; }
        public Nullable<long> Id_NganhNgheTinhNang { get; set; }
        public string SrcImage { get; set; }
        public string Note { get; set; }
    
        public virtual TinhNangNghanhNghe TinhNangNghanhNghe { get; set; }
    }
}
