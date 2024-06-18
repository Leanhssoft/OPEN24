namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class UpdateCoumnNoiDungThu_tblQuyHoaDon : DbMigration
    {
        public override void Up()
        {
            AlterColumn("dbo.Quy_HoaDon", "NoiDungThu", c => c.String(maxLength: 4000));

            Sql(@"ALTER PROCEDURE [dbo].[UpdateChiTietKiemKe_WhenEditCTHD]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDMin [datetime]
AS
BEGIN
    SET NOCOUNT ON;
	
	----declare @IDHoaDonInput uniqueidentifier, @IDChiNhanhInput uniqueidentifier,@NgayLapHDMin  datetime 
	----select top 1 @IDHoaDonInput = ID, @IDChiNhanhInput = ID_DonVi,
	----@NgayLapHDMin = NgayLapHoaDon
	----from BH_HoaDon where MaHoaDon='PKK0000000001'
  
		 ------- get all donviquydoi lienquan ---
			declare @tblQuyDoi table (ID_DonViQuiDoi uniqueidentifier, ID_HangHoa uniqueidentifier, 
				ID_LoHang uniqueidentifier, 
				TyLeChuyenDoi float,
				LaHangHoa bit)
			insert into @tblQuyDoi
			select * from dbo.fnGetAllHangHoa_NeedUpdateTonKhoGiaVon(@IDHoaDonInput)

			------ get all ctKiemKe need update ---
			DECLARE @cthdNeed TABLE (ID_HoaDon UNIQUEIDENTIFIER, ID_ChiTietHoaDon UNIQUEIDENTIFIER, 
			NgayLapHoaDon datetime,SoLuong float, ID_HangHoa UNIQUEIDENTIFIER,
			ID_LoHang UNIQUEIDENTIFIER, ID_DonViQuiDoi UNIQUEIDENTIFIER, TonDauKy float, TyLeChuyenDoi float, TienChietKhau float)
			insert into @cthdNeed
			select 
				hd.ID as ID_HoaDon,
				ct.ID as ID_ChiTietHoaDon,
				hd.NgayLapHoaDon,
				ct.SoLuong,
				qd.ID_HangHoa,
				ct.ID_LoHang,
				ct.ID_DonViQuiDoi,
				0 as TonDauKy,
				qd.TyLeChuyenDoi,
				ct.TienChietKhau
			from BH_HoaDon hd
			join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
			join  @tblQuyDoi qd on ct.ID_DonViQuiDoi= qd.ID_DonViQuiDoi			
				and (ct.ID_LoHang = qd.ID_LoHang or ct.ID_LoHang is null and qd.ID_LoHang is null)
    		WHERE hd.ChoThanhToan = 0 
			AND hd.LoaiHoaDon = 9 
    		and hd.ID_DonVi = @IDChiNhanhInput 
			and hd.NgayLapHoaDon >= @NgayLapHDMin



				----- get tonLuyKe all cthd LienQuan ----			
					select 
						ID_ChiTietHoaDon,
						MaHoaDon,
						ID_LoHang,
						ID_HangHoa,
						TonLuyKe,
						NgayLapHoaDon
					into #cthdLienQuan
					from
					(
					select 
						ct.ID as ID_ChiTietHoaDon,							
						ct.ID_HoaDon,							
						ct.ID_LoHang,
						qd.ID_HangHoa,			
						hd.MaHoaDon,
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' THEN ct.TonLuyKe_NhanChuyenHang ELSE ct.TonLuyKe END as TonLuyKe
					from BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID  		
					join @tblQuyDoi qd on  ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
						and (qd.ID_LoHang = ct.ID_LoHang or (qd.ID_LoHang is null and ct.ID_LoHang is null))
    				WHERE hd.ChoThanhToan = 0    		
						and hd.LoaiHoaDon NOT IN (3, 19, 25,29)					
						and exists (select ctNeed.ID_DonViQuiDoi 
								from @cthdNeed ctNeed 
								where ctNeed.ID_HangHoa = qd.ID_HangHoa 
								and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
								------ chỉ lấy những hóa đơn có ngày lập < ngày kiểm kê (có thể có nhiều khoảng ngày kiểm kê )---
								AND ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    							or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
								)
					)cthdLienQuan
		
			
			

			update ctNeed set ctNeed.TonDauKy = cthd.TonDauKy
				from @cthdNeed ctNeed
				join
				(
					select 
						cthdIn.ID_ChiTietHoaDon,
						cthdIn.TonDauKy
					from
					(
					select ctNeed.ID_ChiTietHoaDon, 
						ctNeed.ID_LoHang,
						ctNeed.ID_HangHoa,
						ctNeed.NgayLapHoaDon,
									
						isnull(tkDK.TonLuyKe,0) as TonDauKy,
						----- Lấy tồn đầu kỳ của từng chi tiết hóa đơn, ưu tiên sắp xếp theo tkDK.NgayLapHoaDon gần nhất (max) ---
						----- vì có thể có nhiều hd < ngaylaphoadon of ctNeed ----
						ROW_NUMBER() over (partition by ctNeed.ID_ChiTietHoaDon order by tkDK.NgayLapHoaDon desc) as RN
					from @cthdNeed ctNeed
					left join #cthdLienQuan tkDK on ctNeed.ID_HangHoa = tkDK.ID_HangHoa 
						and (ctNeed.ID_LoHang = tkDK.ID_LoHang or (ctNeed.ID_LoHang is null and tkDK.ID_LoHang is null)) 
						and tkDK.NgayLapHoaDon < ctNeed.NgayLapHoaDon
					)cthdIn
					where rn = 1
				)cthd on cthd.ID_ChiTietHoaDon = ctNeed.ID_ChiTietHoaDon
					


			---------- update TonkhoDB, SoLuongLech, GiaTriLech to BH_HoaDon_ChiTiet----
			update ctkiemke
			set	ctkiemke.TienChietKhau = ctLast.TonDauKy, 
    			ctkiemke.SoLuong = ctkiemke.ThanhTien - ctLast.TonDauKy, ---- soluonglech
    			ctkiemke.ThanhToan = ctkiemke.GiaVon * (ctkiemke.ThanhTien - ctLast.TonDauKy) --- gtrilech = soluonglech * giavon		
			from BH_HoaDon_ChiTiet ctkiemke
			join  
			(
				select cthd.ID_ChiTietHoaDon, 
					----- phai quydoi TonKho theo dvt ---
					cthd.TonDauKy/cthd.TyLeChuyenDoi as TonDauKy, 
					cthd.ID_HangHoa, cthd.TyLeChuyenDoi
				from @cthdNeed cthd	
				where ROUND(cthd.TonDauKy/ cthd.TyLeChuyenDoi, 3) !=  ROUND(cthd.TienChietKhau, 3) 
			) ctLast on ctkiemke.ID =  ctLast.ID_ChiTietHoaDon


			------------- update TongChenhLech for BH_HoaDon ----
			-------- TongGiamGia: sum(SoLuonglech),
			-------- TongTienThue = sum(GiaTriLech) ---
			-------- TongChiPhi: Tổng số lượng lệch tăng = sum (SoLuong - chỉ lấy SoLuong > 0)
			-------- TongTienHang: Tổng số lượng lệch giảm = sum (SoLuong - chỉ lấy SoLuong < 0)

			update hdKK set 
				hdKK.TongGiamGia = ctKK.SoLuongLech,
				hdKK.TongTienThue = ctKK.GiaTriLech,
				hdKK.TongChiPhi = ctKK.SoLuongLechTang,
				hdKK.TongTienHang = ctKK.SoLuongLechGiam
			from BH_HoaDon hdKK
			join
			(
				select 
					ct.ID_HoaDon,
					sum(ct.SoLuong) as SoLuongLech,
					sum(iif(ct.SoLuong >0, ct.SoLuong,0)) as SoLuongLechTang,
					sum(iif(ct.SoLuong < 0, ct.SoLuong,0)) as SoLuongLechGiam,
					sum(ct.ThanhToan) as GiaTriLech
				from BH_HoaDon_ChiTiet ct
				where exists (select ctNeed.ID_HoaDon from @cthdNeed ctNeed where ctNeed.ID_ChiTietHoaDon = ct.ID)
				group by ct.ID_HoaDon
			)ctKK on hdKK.ID = ctKK.ID_HoaDon		
			
	
			drop table #cthdLienQuan


END");
        }
        
        public override void Down()
        {
            AlterColumn("dbo.Quy_HoaDon", "NoiDungThu", c => c.String(maxLength: 500));
        }
    }
}
