namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class UpdateTable_20230414 : DbMigration
    {
        public override void Up()
        {
            DropForeignKey("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", "dbo.BH_HoaDon_ChiTiet");
            DropIndex("dbo.BH_HoaDon_ChiPhi", new[] { "ID_HoaDon_ChiTiet" });
            AddColumn("dbo.Gara_GiayToKemTheo", "STT", c => c.Int());
            AddColumn("dbo.Gara_HangMucSuaChua", "STT", c => c.Int());
            AlterColumn("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", c => c.Guid());
            CreateIndex("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet");
            AddForeignKey("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", "dbo.BH_HoaDon_ChiTiet", "ID");
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", "dbo.BH_HoaDon_ChiTiet");
            DropIndex("dbo.BH_HoaDon_ChiPhi", new[] { "ID_HoaDon_ChiTiet" });
            AlterColumn("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", c => c.Guid(nullable: false));
            DropColumn("dbo.Gara_HangMucSuaChua", "STT");
            DropColumn("dbo.Gara_GiayToKemTheo", "STT");
            CreateIndex("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet");
            AddForeignKey("dbo.BH_HoaDon_ChiPhi", "ID_HoaDon_ChiTiet", "dbo.BH_HoaDon_ChiTiet", "ID", cascadeDelete: true);
        }
    }
}
