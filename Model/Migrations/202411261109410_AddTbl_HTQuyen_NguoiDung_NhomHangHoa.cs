namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddTbl_HTQuyen_NguoiDung_NhomHangHoa : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.HTQuyen_NguoiDung_NhomHangHoa",
                c => new
                    {
                        ID = c.Guid(nullable: false),
                        ID_NguoiDung = c.Guid(nullable: false),
                        ID_NhomHang = c.Guid(nullable: false),
                    })
                .PrimaryKey(t => t.ID)
                .ForeignKey("dbo.DM_NhomHangHoa", t => t.ID_NhomHang, cascadeDelete: true)
                .ForeignKey("dbo.HT_NguoiDung", t => t.ID_NguoiDung, cascadeDelete: true)
                .Index(t => t.ID_NguoiDung)
                .Index(t => t.ID_NhomHang);
            
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.HTQuyen_NguoiDung_NhomHangHoa", "ID_NguoiDung", "dbo.HT_NguoiDung");
            DropForeignKey("dbo.HTQuyen_NguoiDung_NhomHangHoa", "ID_NhomHang", "dbo.DM_NhomHangHoa");
            DropIndex("dbo.HTQuyen_NguoiDung_NhomHangHoa", new[] { "ID_NhomHang" });
            DropIndex("dbo.HTQuyen_NguoiDung_NhomHangHoa", new[] { "ID_NguoiDung" });
            DropTable("dbo.HTQuyen_NguoiDung_NhomHangHoa");
        }
    }
}
