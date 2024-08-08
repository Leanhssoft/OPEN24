namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddColumnToDM_NganHang : DbMigration
    {
        public override void Up()
        {
            AddColumn("dbo.DM_NganHang", "MaPinNganHang", c => c.String(maxLength: 10));
            AddColumn("dbo.DM_NganHang", "LogoNganHang", c => c.String());
            AddColumn("dbo.DM_NganHang", "TenRutGon", c => c.String(maxLength: 30));
            AddColumn("dbo.DM_NganHang", "TrangThai", c => c.Byte());

            Sql(@"update DM_NganHang set MaPinNganHang='970400', LogoNganHang='https://api.vietqr.io/img/SGICB.png', TenRutGon='SaigonBank', TrangThai = 1 WHERE MaNganHang='SGB';
update DM_NganHang set MaPinNganHang='970432', LogoNganHang='https://api.vietqr.io/img/VPB.png', TenRutGon='VPBank', TrangThai = 1 WHERE MaNganHang='VPBANK';
update DM_NganHang set MaPinNganHang='970408', LogoNganHang='https://api.vietqr.io/img/GPB.png', TenRutGon='GPBank', TrangThai = 1 WHERE MaNganHang='GPBANK';
update DM_NganHang set MaPinNganHang='970430', LogoNganHang='https://api.vietqr.io/img/PGB.png', TenRutGon='PGBank', TrangThai = 1 WHERE MaNganHang='PGBANK';
update DM_NganHang set MaPinNganHang='970448', LogoNganHang='https://api.vietqr.io/img/OCB.png', TenRutGon='OCB', TrangThai = 1 where MaNganHang='OCB';
update DM_NganHang set MaPinNganHang='970452', LogoNganHang='https://api.vietqr.io/img/KLB.png', TenRutGon='KienLongBank', TrangThai = 1 WHERE MaNganHang='KIENLONGBANK';
update DM_NganHang set MaPinNganHang='970414', LogoNganHang='https://api.vietqr.io/img/OCEANBANK.png', TenRutGon='Oceanbank', TrangThai = 1 WHERE MaNganHang='OCEANBANK';
update DM_NganHang set MaPinNganHang='970425', LogoNganHang='https://api.vietqr.io/img/ABB.png', TenRutGon='ABBANK', TrangThai = 1 WHERE MaNganHang='ABBANK';
update DM_NganHang set MaPinNganHang='970426', LogoNganHang='https://api.vietqr.io/img/MSB.png', TenRutGon='MSB', TrangThai = 1 WHERE MaNganHang='MSB';
update DM_NganHang set MaPinNganHang='970441', LogoNganHang='https://api.vietqr.io/img/VIB.png', TenRutGon='VIB', TrangThai = 1 WHERE MaNganHang='VIB';
update DM_NganHang set MaPinNganHang='970436', LogoNganHang='https://api.vietqr.io/img/VCB.png', TenRutGon='Vietcombank', TrangThai = 1 WHERE MaNganHang='VCB';
update DM_NganHang set MaPinNganHang='970422', LogoNganHang='https://api.vietqr.io/img/MB.png', TenRutGon='MBBank', TrangThai = 1 WHERE MaNganHang='MBB';
update DM_NganHang set MaPinNganHang='970403', LogoNganHang='https://api.vietqr.io/img/STB.png', TenRutGon='Sacombank', TrangThai = 1 WHERE MaNganHang='STB';
update DM_NganHang set MaPinNganHang='970443', LogoNganHang='https://api.vietqr.io/img/SHB.png', TenRutGon='SHB', TrangThai = 1 WHERE MaNganHang='SHB';
update DM_NganHang set MaPinNganHang='970416', LogoNganHang='https://api.vietqr.io/img/ACB.png', TenRutGon='ACB', TrangThai = 1 WHERE MaNganHang='ACB';
update DM_NganHang set MaPinNganHang='970419', LogoNganHang='https://api.vietqr.io/img/NCB.png', TenRutGon='NCB', TrangThai = 1 WHERE MaNganHang='NCB';
update DM_NganHang set MaPinNganHang='970454', LogoNganHang='https://api.vietqr.io/img/VCCB.png', TenRutGon='VietCapitalBank', TrangThai = 1 WHERE MaNganHang='VIETCAPITALBANK';
update DM_NganHang set MaPinNganHang='970449', LogoNganHang='https://api.vietqr.io/img/LPB.png', TenRutGon='LienVietPostBank', TrangThai = 1 WHERE MaNganHang='LPB';
update DM_NganHang set MaPinNganHang='970423', LogoNganHang='https://api.vietqr.io/img/TPB.png', TenRutGon='TPBank', TrangThai = 1 WHERE MaNganHang='TPBANK';
update DM_NganHang set MaPinNganHang='970407', LogoNganHang='https://api.vietqr.io/img/TCB.png', TenRutGon='Techcombank', TrangThai = 1 WHERE MaNganHang='TECHCOMBANK';
update DM_NganHang set MaPinNganHang='970431', LogoNganHang='https://api.vietqr.io/img/EIB.png', TenRutGon='Eximbank', TrangThai = 1 WHERE MaNganHang='EIB';
update DM_NganHang set MaPinNganHang='970428', LogoNganHang='https://api.vietqr.io/img/NAB.png', TenRutGon='NamABank', TrangThai = 1 WHERE MaNganHang='NAMABANK';
update DM_NganHang set MaPinNganHang='970429', LogoNganHang='https://api.vietqr.io/img/SCB.png', TenRutGon='SCB', TrangThai = 1 WHERE MaNganHang='SCB';
update DM_NganHang set MaPinNganHang='970409', LogoNganHang='https://api.vietqr.io/img/BAB.png', TenRutGon='BacABank', TrangThai = 1 WHERE MaNganHang='BACABANK';
update DM_NganHang set MaPinNganHang='970437', LogoNganHang='https://api.vietqr.io/img/HDB.png', TenRutGon='HDBank', TrangThai = 1 WHERE MaNganHang='HDBANK';
update DM_NganHang set MaPinNganHang='970405', LogoNganHang='https://api.vietqr.io/img/VBA.png', TenRutGon='Agribank', TrangThai = 1 WHERE MaNganHang='AGRIBANK';
update DM_NganHang set MaPinNganHang='999888', LogoNganHang='https://api.vietqr.io/img/VBSP.png', TenRutGon='VBSP', TrangThai = 1 WHERE MaNganHang='NHCSXH/VBSP';
update DM_NganHang set MaPinNganHang='970418', LogoNganHang='https://api.vietqr.io/img/BIDV.png', TenRutGon='BIDV', TrangThai = 1 WHERE MaNganHang='BIDV';
update DM_NganHang set MaPinNganHang='970440', LogoNganHang='https://api.vietqr.io/img/SEAB.png', TenRutGon='SeABank', TrangThai = 1 WHERE MaNganHang='SEABANK';
update DM_NganHang set MaPinNganHang='970415', LogoNganHang='https://api.vietqr.io/img/ICB.png', TenRutGon='VietinBank', TrangThai = 1 WHERE MaNganHang='CTG';
update DM_NganHang set MaPinNganHang='970427', LogoNganHang='https://api.vietqr.io/img/VAB.png', TenRutGon='VietABank', TrangThai = 1 WHERE MaNganHang='VAB'
update DM_NganHang set MaPinNganHang='970412', LogoNganHang='https://api.vietqr.io/img/PVCB.png', TenRutGon='PVcomBank', TrangThai = 1 WHERE MaNganHang='PVCOMBANK';
update DM_NganHang set MaPinNganHang='970438', LogoNganHang='https://api.vietqr.io/img/BVB.png', TenRutGon='BaoVietBank', TrangThai = 1 WHERE MaNganHang='BVB';
update DM_NganHang set MaPinNganHang='970433', LogoNganHang='https://api.vietqr.io/img/VIETBANK.png', TenRutGon='VietBank', TrangThai = 1 WHERE MaNganHang='VIETBANK';
update DM_NganHang set MaPinNganHang='970444', LogoNganHang='https://api.vietqr.io/img/CBB.png', TenRutGon='CBBank', TrangThai = 1 WHERE MaNganHang='CB';
update DM_NganHang set MaPinNganHang='970406', LogoNganHang='https://api.vietqr.io/img/DOB.png', TenRutGon='DongABank', TrangThai = 1 WHERE MaNganHang='DAF';
");
        }
        
        public override void Down()
        {
            DropColumn("dbo.DM_NganHang", "TrangThai");
            DropColumn("dbo.DM_NganHang", "TenRutGon");
            DropColumn("dbo.DM_NganHang", "LogoNganHang");
            DropColumn("dbo.DM_NganHang", "MaPinNganHang");
        }
    }
}
