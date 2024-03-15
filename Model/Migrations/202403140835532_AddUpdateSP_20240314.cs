namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20240314 : DbMigration
    {
        public override void Up()
        {
			Sql(@"CREATE FUNCTION [dbo].[fnGetAllHangHoa_NeedUpdateTonKhoGiaVon]
(	
	@IDHoaDon uniqueidentifier
)
RETURNS TABLE 
AS
RETURN 
(
	
			select distinct
				qdOut.ID as ID_DonViQuiDoi,
				qdOut.ID_HangHoa,
				qdIn.ID_LoHang,
				qdOut.TyLeChuyenDoi,				
				hh.LaHangHoa

			from
			(
				------ chi get idLohang from bh_chitiet ---
				select 
					qd.ID_HangHoa,
					lo.ID as ID_LoHang
				from
				(
					select ct.ID_DonViQuiDoi, ct.ID_LoHang
					from BH_HoaDon_ChiTiet ct
					where ct.ID_HoaDon = @IDHoaDon 
				)ct
			join DonViQuiDoi qd on qd.ID = ct.ID_DonViQuiDoi
			left join DM_LoHang lo on ct.ID_LoHang = lo.ID or lo.ID is null and ct.ID_LoHang is null
			)qdIn
			join DonViQuiDoi qdOut on qdIn.ID_HangHoa = qdOut.ID_HangHoa
			join DM_HangHoa hh on qdOut.ID_HangHoa = hh.ID and qdIn.ID_HangHoa = hh.ID 
			where LaHangHoa='1'
)
");

			Sql(@"ALTER FUNCTION [dbo].[GetTongTra_byIDHoaDon]
(
	@ID_HoaDon uniqueidentifier,
	@NgayLapHoaDon datetime
)
RETURNS float
AS
BEGIN
		DECLARE @Gtri float=0

		---- get all hdTra of hdgoc with ngaylap < ngaylap of HDTra current --
		declare @tblHDTra table (ID uniqueidentifier, PhaiThanhToan float)
		insert into @tblHDTra
		select ID, PhaiThanhToan
		from BH_HoaDon
		where LoaiHoaDon = 6
		and ChoThanhToan='0'
		and ID_HoaDon= @ID_HoaDon
		and NgayLapHoaDon < @NgayLapHoaDon ---- hd$root: get allHDTra (don't check NgayLap)


		declare @tblHDDoi_fromHdTra table (ID uniqueidentifier, ID_HoaDon uniqueidentifier, PhaiThanhToan float)
		insert into @tblHDDoi_fromHdTra
		select hdDoi.ID,
			hdDoi.ID_HoaDon,
			hdDoi.PhaiThanhToan
		from BH_HoaDon hdDoi
		where LoaiHoaDon in (1,19)
		and ChoThanhToan='0'
		and exists (select id from @tblHDTra hdTra where hdDoi.ID_HoaDon = hdTra.ID)
		and hdDoi.NgayLapHoaDon < @NgayLapHoaDon

	set @Gtri = (
					select 					
						sum(PhaiThanhToan + DaTraKhach + NoHDDoi) as NoKhach
					from
					(
						select ID,
							-PhaiThanhToan as PhaiThanhToan, 
							0 as DaTraKhach,
							0 as NoHDDoi
						from @tblHDTra					

						union all
						---- get phieuchi hdTra ----
						select 
							hdt.ID,
							0 as PhaiThanhToan,
							iif(qhd.LoaiHoaDon=12, qct.TienThu, -qct.TienThu) as DaTraKhach,
							0 as NoHDDoi
						from @tblHDTra hdt
						join Quy_HoaDon_ChiTiet qct on hdt.ID = qct.ID_HoaDonLienQuan
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
						where qhd.TrangThai='1'
						and qhd.NgayLapHoaDon < @NgayLapHoaDon

						union all
						---- get all HDDoifrom hdTra ----						
						select 
							hdDoi.ID_HoaDon,	
							0 as PhaiThanhToan,
							0 as DaTraKhach,
							sum(hdDoi.PhaiThanhToan) - sum(ISNULL(DaTraKhach,0)) as NoHDDoi
						from @tblHDDoi_fromHdTra hdDoi
						left join
						(
							---- all phieuthu of hdDoi ---
							select 
								hdDoi.ID,
								iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu) as DaTraKhach
							from @tblHDDoi_fromHdTra hdDoi
							join Quy_HoaDon_ChiTiet qct on hdDoi.ID = qct.ID_HoaDonLienQuan
							join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
							where qhd.TrangThai='1'
						)sq on hdDoi.ID = sq.ID
						group by hdDoi.ID_HoaDon											

					) tblThuChi
		)
	RETURN @Gtri 

END");

			Sql(@"ALTER FUNCTION [dbo].[BuTruTraHang_HDDoi]
(
	@ID_HoaDon uniqueidentifier,
	@NgayLapHoaDon datetime,
	@ID_HoaDonGoc uniqueidentifier = null,
	@LoaiHDGoc int =  0	
)
RETURNS float
AS
BEGIN

			
	DECLARE @Gtri float=0


	----- neu gtributru > 0 --> khong butru
	set @Gtri = (		
				select 
					sum(isnull(PhaiThanhToan,0) + isnull(DaThanhToan,0)) as BuTruTra
				from
				(
				select 		
					hd.ID,					
					iif(hd.LoaiHoaDon =6, -hd.PhaiThanhToan, hd.PhaiThanhToan)  -- 0  as PhaiThanhToan
						+ isnull((select dbo.BuTruTraHang_HDDoi(hd.ID_HoaDon, hd.NgayLapHoaDon, hdgoc.ID_HoaDon, hdgoc.LoaiHoaDon)),0) --- hdTra tructiep cua hdDoi + hdGoc
						+ isnull((select dbo.GetTongTra_byIDHoaDon(hd.ID_HoaDon, hd.NgayLapHoaDon)),0) --- allHDTra + chilienquan					
						as PhaiThanhToan,
					0 as DaThanhToan
				from BH_HoaDon hd
				left join BH_HoaDon hdgoc on hd.ID_HoaDon = hdgoc.ID
				where hd.ChoThanhToan='0'
				and hd.LoaiHoaDon in (1,6,19,25)
				and hd.ID= @ID_HoaDon
				and hd.NgayLapHoaDon < @NgayLapHoaDon

				

				union all

				 ------ get phieuthu/chi of hd (tra/hdgoc) duoc truyen vao ---		
				 ------ neu HDxuly from baogia,  not get phieuthu of baogia
				select 
					qct.ID_HoaDonLienQuan,
					0 as PhaiThanhToan, 
					iif(@LoaiHDGoc=3,0,	iif(qhd.LoaiHoaDon=12,  qct.TienThu, -qct.TienThu)) as DaThanhToan
				from Quy_HoaDon_ChiTiet qct				
				join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
				where qhd.TrangThai='1'
				and qct.ID_HoaDonLienQuan= @ID_HoaDon				
			
				
				union all

				---- thu dathang hdgoc ---
				select 
					allXL.ID,
					0 as PhaiThanhToan, 
					allXL.ThuDatHang
				from
				(
						select hdXl.ID, 
							hdXl.ID_HoaDon, 
							hdXl.NgayLapHoaDon ,
							hdXl.PhaiThanhToan,
							ROW_NUMBER() over (order by hdXl.NgayLapHoaDon) as RN,
							isnull(ThuDatHang,0) as ThuDatHang
						from BH_HoaDon hdXl
						left join(
							select 
								@ID_HoaDonGoc as ID_HoaDonLienQuan,
								sum(iif(qhd.LoaiHoaDon =12, qct.TienThu,  -qct.TienThu)) as ThuDatHang
							from Quy_HoaDon qhd
							join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
							where qhd.TrangThai='1'
							and qct.ID_HoaDonLienQuan= @ID_HoaDonGoc
						) thuDH on thuDH.ID_HoaDonLienQuan = hdXl.ID_HoaDon
						where  hdXl.ID_HoaDon= @ID_HoaDonGoc ---- get all HDxuly from baogia						
						and hdXl.LoaiHoaDon in (1,25)
						and hdXl.ChoThanhToan= '0'
					) allXL where allXL.RN= 1
					and allXL.ID= @ID_HoaDon
			) tbl
		)
	
	RETURN @Gtri
END");

			CreateStoredProcedure(name: "[dbo].[GetQuyen_ByIDNguoiDung]", parametersAction: p => new
			{
				ID_NguoiDung = p.String(),
				ID_DonVi = p.String()
			}, body: @"SET NOCOUNT ON;
    DECLARE @LaAdmin bit
    
    	select top 1 @LaAdmin =  LaAdmin from HT_NguoiDung where ID like @ID_NguoiDung
    
    	-- LaAdmin: full quyen, assign ID = neID() --> because class HT_Quyen_NhomDTO {ID, MaQuyen}
    	if @LaAdmin	='1'
    		select NEWID() as ID,  MaQuyen from HT_Quyen where DuocSuDung = '1'
    	else	
    		select NEWID() as  ID, MaQuyen 
    		from HT_NguoiDung_Nhom nnd
    		JOIN HT_Quyen_Nhom qn on nnd.IDNhomNguoiDung = qn.ID_NhomNguoiDung
    		where nnd.IDNguoiDung like @ID_NguoiDung and nnd.ID_DonVi like @ID_DonVi");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateTonLuyKeCTHD_whenUpdate]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDOld [datetime]
AS
BEGIN
    SET NOCOUNT ON;

	--declare @IDHoaDonInput uniqueidentifier, @IDChiNhanhInput uniqueidentifier, @NgayLapHDOld datetime

	--select top 1 @IDHoaDonInput = ID, @IDChiNhanhInput = ID_DonVi, @NgayLapHDOld = NgayLapHoaDon  
	--from BH_HoaDon where MaHoaDon='PKK321102700084'


    
    		declare @NgayLapHDNew DATETIME, @NgayLapHDMin DATETIME, @LoaiHoaDon INT;   
    		declare @tblHoaDonChiTiet ChiTietHoaDonEdit -- chỉ dùng để Insert_ThongBaoHetTonKho ---
    
    		-----  get NgayLapHD by IDHoaDon: if update HDNhanHang (loai 10, yeucau = 4 --> get NgaySua
    		select 
    			@NgayLapHDNew = NgayLapHoaDon,
    			@LoaiHoaDon = LoaiHoaDon
    		from (
    					select LoaiHoaDon,
							case when @IDChiNhanhInput = ID_CheckIn and YeuCau !='1' then NgaySua else NgayLapHoaDon end as NgayLapHoaDon
    					from BH_HoaDon where ID = @IDHoaDonInput
				) a
    
    		-- alway get Ngay min --> compare to update TonLuyKe
    		IF(@NgayLapHDOld > @NgayLapHDNew)
    			SET @NgayLapHDMin = @NgayLapHDNew;
    		ELSE
    			SET @NgayLapHDMin = @NgayLapHDOld;
    
			declare @NgayLapHDMin_SubMiliSecond datetime = dateadd(MILLISECOND,-3, @NgayLapHDMin)

		
			 ----- get all donviquydoi lienquan ---
			declare @tblQuyDoi table (ID_DonViQuiDoi uniqueidentifier, ID_HangHoa uniqueidentifier, 
				ID_LoHang uniqueidentifier, 
				TyLeChuyenDoi float,
				LaHangHoa bit)
			insert into @tblQuyDoi
			select * from dbo.fnGetAllHangHoa_NeedUpdateTonKhoGiaVon(@IDHoaDonInput)

    		
    		------ get all cthd need update TonKho (>= ngayLapHoaDon of hdCurrent) -----
    		select
    			ct.ID, 
				ct.ID_HoaDon,
    			ct.ID_LoHang,
				ct.ID_DonViQuiDoi,
				-- chatlieu = 5 (cthd bi xoa khi updateHD), chatlieu =2 (tra gdv  - khong cong lai tonkho)
    			case when ct.ChatLieu= '5' or ct.ChatLieu ='2' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' then 0 else SoLuong end as SoLuong, 
    			case when ct.ChatLieu= '5' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' then 0 else TienChietKhau end as TienChietKhau,
    			case when ct.ChatLieu= '5' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' then 0 else ct.ThanhTien end as ThanhTien,-- kiemke bi huy
				----- chỉ cần lấy TonLuyLe của phiếu kiếm kê, vì các loại khác sẽ bị tính lại TonKho ---
				iif(hd.LoaiHoaDon = 9,iif(hd.ChoThanhToan is null or hd.ChoThanhToan ='1',0, ct.ThanhTien * qd.TyLeChuyenDoi),0) as TonLuyKe,
				qd.ID_HangHoa,
    			qd.TyLeChuyenDoi,
    			hd.MaHoaDon,
    			hd.LoaiHoaDon,
    			hd.ID_DonVi,
    			hd.ID_CheckIn,
    			hd.YeuCau,				
				hd.ChoThanhToan,
    			case when hd.YeuCau = '4' AND hd.ID_CheckIn = @IDChiNhanhInput then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon
    		into #temp
    		from BH_HoaDon_ChiTiet ct
			join BH_HoaDon hd on ct.ID_HoaDon = hd.ID   
			join @tblQuyDoi qd on qd.ID_DonViQuiDoi = ct.ID_DonViQuiDoi and (ct.ID_LoHang = qd.ID_LoHang or ct.ID_LoHang is null and qd.ID_LoHang is null)
    		WHERE hd.ID = @IDHoaDonInput
			----- chỉ update TonKho cho hdCurrent/ hoặc hóa đơn chưa hủy có >= ngayLapHoaDon of hdCurrent
			or (hd.ChoThanhToan='0' and 
					((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon > @NgayLapHDMin_SubMiliSecond
    				and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    				or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and hd.NgaySua > @NgayLapHDMin_SubMiliSecond))
					)
					
				
			------ TonDauKy of hanghoa (id_hanghoa, id_lohang) ----			
				select *
				into #tblTonKhoDK
				from
				(
					select 
						tblTonKho.ID_HangHoa,
						tblTonKho.ID_LoHang,					
						tblTonKho.TonLuyKe,
						row_number() over (partition by tblTonKho.ID_HangHoa,tblTonKho.ID_LoHang order by tblTonKho.NgayLapHoaDon desc) as RN	
					from
					(
					select 				
						ct.ID_LoHang,
						qd.ID_HangHoa,				
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						CASE WHEN @IDChiNhanhInput = hd.ID_CheckIn and hd.YeuCau = '4' THEN ct.TonLuyKe_NhanChuyenHang ELSE ct.TonLuyKe END as TonLuyKe
					from BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID  
					join @tblQuyDoi qd on ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
    				WHERE hd.ChoThanhToan = 0    		
						and hd.LoaiHoaDon NOT IN (3, 19, 25,29) ---- 29. Phiếu khởi tạo phù tùng xe		
						------ chỉ lấy hd trước đó
						and ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon < @NgayLapHDMin
    					and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    						or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and hd.NgaySua < @NgayLapHDMin))
					)tblTonKho
				)tblTonKhoDK where tblTonKhoDK.RN =1

				

				------ get all PhieuKiemKe  ----
				declare @tblPhieuKiemKe table (ID_HoaDon uniqueidentifier, NgayKiemKe datetime)
				insert into @tblPhieuKiemKe
				select ID_HoaDon, NgayLapHoaDon from #temp 
				---- chỉ lấy phiếu kiểm kê chưa bị hủy ---
				where LoaiHoaDon = 9 and ChoThanhToan = 0 group by ID_HoaDon, NgayLapHoaDon

				declare @countKiemKe float = (select count(*) from @tblPhieuKiemKe)

				------- get allhanghoa has kiemke ---
				select 
					ID_HangHoa, ID_LoHang
				into #hangKiemKe
				from #temp
				where LoaiHoaDon = 9 and ChoThanhToan  = 0
				group by ID_HangHoa, ID_LoHang

			

				------ get cthd has hangKiemKe ---
				select *
				into #cthdHasKiemKe
				from #temp ct
				where ct.LoaiHoaDon != 9 and
				exists 
					(select ID_HangHoa from #hangKiemKe hKK 
					where ct.ID_HangHoa = hKK.ID_HangHoa 
							and (ct.ID_LoHang = hKK.ID_LoHang or ct.ID_LoHang is null and hKK.ID_LoHang is null))


				declare @tblCTHDAfter table (ID_ChiTietHD uniqueidentifier, ID_HoaDon uniqueidentifier, LoaiHoaDon int, MaHoaDon nvarchar (100), NgayLapHoaDon datetime,
							ID_DonVi uniqueidentifier, ID_Checkin uniqueidentifier, YeuCau nvarchar(max),
							ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier, TonLuyKe float)	

				if @countKiemKe >0
				begin
																
						------ duyet phieuKiemKe  --
						declare @idHoaDon uniqueidentifier, @ngayKiemKe datetime
						declare _curKK cursor for
						select ID_HoaDon, NgayKiemKe from @tblPhieuKiemKe order by NgayKiemKe
						open _curKK
						FETCH NEXT FROM _curKK
						INTO @idHoaDon, @ngayKiemKe
						WHILE @@FETCH_STATUS = 0
						BEGIN   
							
								----- tinh TonLuyKe theo giai doan kiem ke
								insert into @tblCTHDAfter
								select 
									ct.ID, ct.ID_HoaDon,
									ct.LoaiHoaDon, ct.MaHoaDon, ct.NgayLapHoaDon,
									ct.ID_DonVi, ct.ID_CheckIn, ct.YeuCau,
									ct.ID_HangHoa, ct.ID_LoHang,												
    							ISNULL(ctKK.TonLuyKe, 0) + 
    								(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * ct.SoLuong* ct.TyLeChuyenDoi, 
    							IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    								IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    							IIF(ct.LoaiHoaDon = 10 AND ct.YeuCau = '4' AND ct.ID_CheckIn = @IDChiNhanhInput, ct.TienChietKhau* ct.TyLeChuyenDoi, 0))))) 
    								OVER(PARTITION BY ct.ID_HangHoa, ct.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe  
								from #cthdHasKiemKe ct
								left join
								(
									------ get tondauky from phieuKiemKe ----
									select 
										ctAll.ID_HangHoa,
										ctAll.ID_LoHang,
										ctAll.TonLuyKe
									from #temp ctAll where ctAll.ID_HoaDon = @idHoaDon
								)ctKK on ct.ID_HangHoa = ctKK.ID_HangHoa
								and (ct.ID_LoHang = ctKK.ID_LoHang or ct.ID_LoHang is null and ctKK.ID_LoHang is null)
								where ct.NgayLapHoaDon >= @ngayKiemKe			

								
		
						FETCH NEXT FROM _curKK
						INTO @idHoaDon, @ngayKiemKe

						END
						CLOSE _curKK;
						DEALLOCATE _curKK;					
				end

		

				------ get cthd conLai (not exists hangKiemKe) && tinhton ---
				----- neu khong co phieuKK: đây là ctAll ---
				----- nguoclai: ctALL trừ ctKiemKe
				insert into @tblCTHDAfter
				select 
					ct.ID, ct.ID_HoaDon,
					ct.LoaiHoaDon, ct.MaHoaDon, ct.NgayLapHoaDon,
					ct.ID_DonVi, ct.ID_CheckIn, ct.YeuCau,
					ct.ID_HangHoa, ct.ID_LoHang,												
    			ISNULL(lkdk.TonLuyKe, 0) + 
    				(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * ct.SoLuong* ct.TyLeChuyenDoi, 
    			IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    				IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    			IIF(ct.LoaiHoaDon = 10 AND ct.YeuCau = '4' AND ct.ID_CheckIn = @IDChiNhanhInput, ct.TienChietKhau* ct.TyLeChuyenDoi, 0))))) 
    				OVER(PARTITION BY ct.ID_HangHoa, ct.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe  
				from #temp ct
				left join #tblTonKhoDK lkdk on ct.ID_HangHoa = lkdk.ID_HangHoa 
					and (ct.ID_LoHang = lkdk.ID_LoHang or ct.ID_LoHang is null and lkdk.ID_LoHang is null)
				where not exists (select id from @tblCTHDAfter ctIn where ct.ID = ctIn.ID_ChiTietHD )
				------ tính lại tồn kho cho phiếu kiểm kê bị hủy ---
				and (ct.LoaiHoaDon !=9 or (ct.LoaiHoaDon = 9 and (ct.ChoThanhToan is null or ChoThanhToan ='1')))


				------- vì @tblCTHDAfter không bao gồm phiếu kiểm kê: phải insert ---> tính Tồn kho hiện tại
					insert into @tblCTHDAfter
					select 
						ct.ID, ct.ID_HoaDon,
						ct.LoaiHoaDon, ct.MaHoaDon, ct.NgayLapHoaDon,
						ct.ID_DonVi, ct.ID_CheckIn, ct.YeuCau,
						ct.ID_HangHoa, ct.ID_LoHang,
						ct.TonLuyKe
					from #temp ct
					where ct.LoaiHoaDon = 9 and ChoThanhToan = 0	



			---select * from @tblCTHDAfter order by NgayLapHoaDon 						

				------ update again TonLuyKe for HoaDon_ChiTiet  -----					
    			UPDATE hdct
    			SET hdct.TonLuyKe = IIF(tlkupdate.ID_DonVi = @IDChiNhanhInput, tlkupdate.TonLuyKe, hdct.TonLuyKe), 
    				hdct.TonLuyKe_NhanChuyenHang = IIF(tlkupdate.ID_CheckIn = @IDChiNhanhInput and tlkupdate.LoaiHoaDon = 10, tlkupdate.TonLuyKe, hdct.TonLuyKe_NhanChuyenHang)
    			FROM BH_HoaDon_ChiTiet hdct
    			JOIN @tblCTHDAfter tlkupdate ON hdct.ID = tlkupdate.ID_ChiTietHD 


				----- get TonKho hientai full ID_QuiDoi, ID_LoHang of ID_HangHoa ----
				DECLARE @tblTonKhoNow TABLE(ID_DonViQuiDoi UNIQUEIDENTIFIER,ID_LoHang UNIQUEIDENTIFIER, TonKho FLOAT)
				insert into @tblTonKhoNow
				select 
					qd.ID_DonViQuiDoi,
					tkNow.ID_LoHang,					
					tkNow.TonLuyKe / qd.TyLeChuyenDoi as TonLuyKeNow --- tinh TonKuyke theo DVT
				from(
					select ID_HangHoa, ID_LoHang,
						TonLuyKe,
						row_number() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon desc) as RN	
					from @tblCTHDAfter
				)tkNow
				join @tblQuyDoi qd on tkNow.ID_HangHoa = qd.ID_HangHoa 
					and (tkNow.ID_LoHang = qd.ID_LoHang or tkNow.ID_LoHang is null and qd.ID_LoHang is null)
				where tkNow.RN= 1



				------ UPDATE TonKho in DM_HangHoa_TonKho -----
				UPDATE hhtonkho SET hhtonkho.TonKho = ISNULL(tkNow.TonKho, 0)
    			FROM DM_HangHoa_TonKho hhtonkho
    			JOIN @tblTonKhoNow tkNow on hhtonkho.ID_DonViQuyDoi = tkNow.ID_DonViQuiDoi 
    			and (hhtonkho.ID_LoHang = tkNow.ID_LoHang or tkNow.ID_LoHang is null)
				and hhtonkho.ID_DonVi = @IDChiNhanhInput


				------ insert DM_TonKho if not exist ----
				INSERT INTO DM_HangHoa_TonKho(ID, ID_DonVi, ID_DonViQuyDoi, ID_LoHang, TonKho)
				select 
				newID(),
				@IDChiNhanhInput,
				tkNow.ID_DonViQuiDoi,
				tkNow.ID_LoHang,
				tkNow.TonKho
				from @tblTonKhoNow tkNow
				where not exists (
					select id from DM_HangHoa_TonKho tk
					where tk.ID_DonViQuyDoi = tkNow.ID_DonViQuiDoi
					and (tk.ID_LoHang = tkNow.ID_LoHang or tkNow.ID_LoHang is null and tk.ID_LoHang is null)
					and tk.ID_DonVi = @IDChiNhanhInput
				)

			------- insert Thongbao het tonkho ----
			begin try
				------- get hanghoa ----
				insert into @tblHoaDonChiTiet (ID_DonViQuiDoi, ID_HangHoa, ID_LoHang, TyLeChuyenDoi)
				select ID_DonViQuiDoi, ID_HangHoa, ID_LoHang, TyLeChuyenDoi from @tblQuyDoi
				
				exec Insert_ThongBaoHetTonKho @IDChiNhanhInput, @LoaiHoaDon, @tblHoaDonChiTiet
			end try
			begin catch
			end catch

    
    	
    		 ------- neu update NhanHang --> goi ham update TonKho 2 lan
    		 ------ update GiaVon neu tontai phieu NhapHang,ChuyenHang/NhanHang, DieuChinhGiaVon 
    		declare @count2 float = (select count(ID_HoaDon) from #temp where LoaiHoaDon in (4,7,10, 18))
    		select ISNULL(@count2,0) as UpdateGiaVon, ISNULL(@countKiemKe,0) as UpdateKiemKe, @NgayLapHDMin as NgayLapHDMin

			
			--drop table @tblQuyDoi
			--drop table #temp
			--drop table #tblTonKhoDK
			--drop table #hangKiemKe
			--drop table #cthdHasKiemKe
			
	

END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateGiaVon_WhenEditCTHD]
   @IDHoaDonInput [uniqueidentifier],
   @IDChiNhanh [uniqueidentifier],
   @NgayLapHDMin [datetime] ----  @NgayLapHDMin: đang lấy ngày lập hóa đơn cũ
AS
BEGIN
    SET NOCOUNT ON;	


			declare @NgayLapHDNew DATETIME, @NgayLapHDMinNew DATETIME, @LoaiHoaDonThis int
			select 
    			@NgayLapHDNew = NgayLapHoaDon,
				@LoaiHoaDonThis = LoaiHoaDon
    		from (
    				select
						LoaiHoaDon,
						case when YeuCau = '4' AND @IDChiNhanh = ID_CheckIn then NgaySua else NgayLapHoaDon end as NgayLapHoaDon
    				from BH_HoaDon where ID = @IDHoaDonInput
				) hdupdate
		
			----- nếu ngày cũ > ngày mới: lấy ngày mới ---
			IF(@NgayLapHDMin > @NgayLapHDNew)
    			SET @NgayLapHDMinNew = @NgayLapHDNew;
    		ELSE
				---- else: lấy ngày cũ ---
    			SET @NgayLapHDMinNew = @NgayLapHDMin;

	
		DECLARE @TinhGiaVonTrungBinh BIT =(SELECT top 1 GiaVonTrungBinh FROM HT_CauHinhPhanMem WHERE ID_DonVi = @IDChiNhanh)  	
		---- khong update giavon cho LoaiHD: 3,19,25 --- vì giá vốn của các loại này = sum(hd sử dụng, hdle, hdxuatkho) ---
		------> giảm bớt số lần gọi 
		IF(@TinhGiaVonTrungBinh IS NOT NULL AND @TinhGiaVonTrungBinh = 1  and @LoaiHoaDonThis not in (3,19, 25))
		BEGIN ----- beginOut

				declare @NgayLapHDMin_SubMiliSecond datetime = dateadd(MILLISECOND,-3, @NgayLapHDMinNew)

				 ----- get all donviquydoi lienquan ---
				declare @tblQuyDoi table (ID_DonViQuiDoi uniqueidentifier, 
					ID_HangHoa uniqueidentifier, 
					ID_LoHang uniqueidentifier, 
					TyLeChuyenDoi float,
					LaHangHoa bit)
				insert into @tblQuyDoi
				select * from dbo.fnGetAllHangHoa_NeedUpdateTonKhoGiaVon(@IDHoaDonInput)
			

    		------ get @cthd_NeedUpGiaVon (ngayLap >= ngaylapMin): từ hóa đơn hiện tại trở về sau ----
    		DECLARE @cthd_NeedUpGiaVon TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonGoc UNIQUEIDENTIFIER, 
					MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, ChoThanhToan bit,
					ID_ChiTietHoaDon UNIQUEIDENTIFIER,
					NgayLapHoaDon DATETIME,
					SoThuThu INT, SoLuong FLOAT, DonGia FLOAT, 
					TongTienHang FLOAT, TongChiPhi FLOAT,
    				ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT,
					TonKho FLOAT,  GiaVon FLOAT, GiaVonNhan FLOAT,
					ID_HangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, 
					IDDonViQuiDoi UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER, 
    				ID_ChiNhanhThemMoi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX),
					GiaVonDauKy float,
					ChatLieu varchar(100),
					IdHoaDonTruocDo UNIQUEIDENTIFIER,
					RN int)
    	INSERT INTO @cthd_NeedUpGiaVon  	
		select ctNeed.*,
			------ rn: get sau khi lay ngaylap/ngaysua --
			ROW_NUMBER() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon) as RN
		from
		(
		select hd.ID as IDHoaDon,
			hd.ID_HoaDon as IDHoaDonGoc, 
			hd.MaHoaDon, 
			hd.LoaiHoaDon,
			hd.ChoThanhToan,
			ct.ID as ID_ChiTietHoaDon, 
    		CASE WHEN hd.YeuCau = '4' AND @IDChiNhanh = hd.ID_CheckIn THEN hd.NgaySua ELSE hd.NgayLapHoaDon END AS NgayLapHoaDon, 				    			    				    							    			
    		ct.SoThuTu,
			iif(ct.ChatLieu='5' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' ,0,ct.SoLuong) as SoLuong, 
			ct.DonGia, 
			iif(hd.ChoThanhToan is null or hd.ChoThanhToan ='1',0, hd.TongTienHang) as TongTienHang,
			isnull(hd.TongChiPhi,0) as TongChiPhi,
			iif(ct.ChatLieu='5' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' ,0,ct.TienChietKhau) as TienChietKhau,
			iif(ct.ChatLieu='5' or hd.ChoThanhToan is null or hd.ChoThanhToan ='1' ,0,ct.ThanhTien) as ThanhTien,
			hd.TongGiamGia, 
			qd.TyLeChuyenDoi,
			0 as TonKho, ---- tạm thời gán = 0, và get lại TonKho đầu kỳ sau --- 	    	
    		iif(hd.ChoThanhToan is null,0,ct.GiaVon / qd.TyLeChuyenDoi) as GiaVon, 
    		iif(hd.ChoThanhToan is null,0,ct.GiaVon_NhanChuyenHang / qd.TyLeChuyenDoi) as GiaVon_NhanChuyenHang,
    		qd.ID_HangHoa, 
			qd.LaHangHoa, 
			qd.ID_DonViQuiDoi, 
			ct.ID_LoHang, 
			ct.ID_ChiTietDinhLuong, 
    		@IDChiNhanh as IDChiNhanh,
			hd.ID_CheckIn,
			hd.YeuCau,
			0 as GiaVonDauKy,
			isnull(ct.ChatLieu,'') as ChatLieu,
			hd.ID as IdHoaDonTruocDo			
    	FROM BH_HoaDon_ChiTiet ct 
		join @tblQuyDoi qd ON ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi and (ct.ID_LoHang = qd.ID_LoHang OR qd.ID_LoHang IS NULL and ct.ID_LoHang is null)
    	INNER JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID   
			----- chỉ update GiaVon cho hdCurrent/ hoặc hóa đơn chưa hủy có >= ngayLapHoaDon of hdCurrent
    	WHERE hd.ID = @IDHoaDonInput
		or (hd.ChoThanhToan= 0
			and hd.id != @IDHoaDonInput
			and hd.LoaiHoaDon NOT IN (3, 19, 29,25)
				------- yeuCau = 3 & LoaiHD = 10: phiếu chuyển hàng bị hủy --
			and	((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon >= @NgayLapHDMin
    				and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    				or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and hd.NgaySua >= @NgayLapHDMin)))			
		)ctNeed

				
				------ tonluyke trước đó của từng hóa đơn ----					
				select 					
					tblTonLuyKe.ID_LoHang,
					tblTonLuyKe.ID_HangHoa,
					tblTonLuyKe.TonLuyKe,
					tblTonLuyKe.NgayLapHoaDon
					into #tblTonLuyKe
				from
				(
				
					select ct.ID_HoaDon,					
						ct.ID_LoHang,
						qd.ID_HangHoa,		
						hd.MahoaDon,					
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' THEN ct.TonLuyKe_NhanChuyenHang ELSE ct.TonLuyKe END as TonLuyKe
					from BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID  
					join @tblQuyDoi qd on ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
					and (qd.ID_LoHang = ct.ID_LoHang or (qd.ID_LoHang is null and ct.ID_LoHang is null))
    				WHERE hd.ChoThanhToan = 0    		
						and hd.LoaiHoaDon NOT IN (3, 19, 25,29)					
						and exists (select ctNeed.IDDonViQuiDoi 
								from @cthd_NeedUpGiaVon ctNeed 
								where ctNeed.ID_HangHoa = qd.ID_HangHoa 
								and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
								------ !important: so sánh ngày lập --> để lấy TonLuyke ---
								AND ((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    							or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
								)					

					)tblTonLuyKe


			------- nếu @ctNeed có chứa loại = 6, và idHoaDonGoc is not null ----
			------- get all cthd Mua gốc ban đầu (theo hàng hóa)---> để lấy giá vốn lúc mua ---
			declare @tblCTMuaGoc table (ID_HDMuaGoc uniqueidentifier, NgayLapHoaDon datetime, LoaiHoaDon int,
				ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier, GiaVon float)

				select ID_ChiTietHoaDon, IDHoaDon,NgayLapHoaDon,
					ID_HangHoa, IDDonViQuiDoi, ID_LoHang
				into #ctTraHang
				from @cthd_NeedUpGiaVon
					where LoaiHoaDon = 6
					and IDHoaDonGoc is not null
				declare @countTraHang  int = (select count(ID_ChiTietHoaDon) from #ctTraHang)
				
			if @countTraHang > 0
				begin
					------- vì HDSC xuất kho nhiều lần: nên nếu Trả hàng từ HDSC --> không thể tính chính xác giá vốn lúc xuất kho ---
					------ (giá vốn dc tính ở trường hợp này chỉ mang tính chất tham khảo: đang lấy GV trước thời điểm tạo HDSC) --
					insert into @tblCTMuaGoc
					select 
						hd.ID,
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						hd.LoaiHoaDon,
						qd.ID_HangHoa,
						ct.ID_LoHang,									
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' then ct.GiaVon_NhanChuyenHang / qd.TyLeChuyenDoi					
    						else ct.GiaVon / qd.TyLeChuyenDoi end as GiaVon
					from BH_HoaDon hd
					join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
					join @tblQuyDoi qd on ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
					and (qd.ID_LoHang = ct.ID_LoHang or (qd.ID_LoHang is null and ct.ID_LoHang is null))
					where hd.ChoThanhToan='0'
					and exists (select ctNeed.ID_HangHoa 
								from #ctTraHang ctNeed 
								where ctNeed.ID_HangHoa = qd.ID_HangHoa
								and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
								AND ((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    							or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
								)	 
				end


				

			------ get giavondauky ----
			select 
				gvDK.ID_HangHoa,
				gvDK.ID_LoHang,
				gvDK.GiaVonDauKy
			into #tblGVDauKy
			from
			(
				select 
					tblGV.ID_HangHoa,
					tblGV.ID_LoHang,
					tblGV.GiaVonDauKy,
					ROW_NUMBER() over (partition by tblGV.ID_HangHoa, tblGV.ID_LoHang order by tblGV.NgayLapHoaDon desc) as RN
				from
				(
				select   					
						ct.ID_LoHang,
						qd.ID_HangHoa,				
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon,
						CASE WHEN @IDChiNhanh = hd.ID_CheckIn and hd.YeuCau = '4' THEN ct.GiaVon_NhanChuyenHang/qd.TyLeChuyenDoi ELSE ct.GiaVon/qd.TyLeChuyenDoi END as GiaVonDauKy
				FROM BH_HoaDon hd
    			INNER JOIN BH_HoaDon_ChiTiet ct ON hd.ID = ct.ID_HoaDon  
				join @tblQuyDoi qd ON ct.ID_DonViQuiDoi = qd.ID_DonViQuiDoi and (ct.ID_LoHang = qd.ID_LoHang OR qd.ID_LoHang IS NULL and ct.ID_LoHang is null)    		
    			WHERE hd.ChoThanhToan = 0 
					and hd.LoaiHoaDon NOT IN (3, 19, 25,29)
    					AND ((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon < @NgayLapHDMinNew and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    						or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and hd.NgaySua < @NgayLapHDMinNew))

				) tblGV
			) gvDK where gvDK.RN= 1


			----- update again TonLuyKe for each ctNeedUpdate ----		
				update ctNeed set ctNeed.TonKho = cthd.TonDauKy
				from @cthd_NeedUpGiaVon ctNeed
				join
				(
					select 
						cthdIn.ID_ChiTietHoaDon,
						cthdIn.TonDauKy
					from
					(
					select ctNeed.ID_ChiTietHoaDon, ctNeed.MaHoaDon,
						ctNeed.ID_LoHang,
						ctNeed.ID_HangHoa,
						ctNeed.NgayLapHoaDon,
						ctNeed.IDHoaDon,				
						isnull(tkDK.TonLuyKe,0) as TonDauKy,
						----- Lấy tồn đầu kỳ của từng chi tiết hóa đơn, ưu tiên sắp xếp theo tkDK.NgayLapHoaDon gần nhất (max) ---
						----- vì có thể có nhiều hd < ngaylaphoadon of ctNeed ----
						ROW_NUMBER() over (partition by ctNeed.ID_ChiTietHoaDon order by tkDK.NgayLapHoaDon desc) as RN
					from @cthd_NeedUpGiaVon ctNeed
					left join #tblTonLuyKe tkDK on ctNeed.ID_HangHoa = tkDK.ID_HangHoa 
						and (ctNeed.ID_LoHang = tkDK.ID_LoHang or (ctNeed.ID_LoHang is null and tkDK.ID_LoHang is null)) 
						and tkDK.NgayLapHoaDon < ctNeed.NgayLapHoaDon
					)cthdIn
					where rn = 1
				)cthd on cthd.ID_ChiTietHoaDon = ctNeed.ID_ChiTietHoaDon

		
		
			----- update idHoaDonTruocDo for ctNeed: nếu trong 1 hóa đơn có cùng hàng hóa, giống/# đơn vị tính
			----- giá vốn cuối cùng được update = giá vốn của RN đầu tiên thuộc cùng hóa đơn ---
			update ctNeed 
				set ctNeed.IdHoaDonTruocDo = ctNeed2.IdHoaDonTruocDo
			from @cthd_NeedUpGiaVon ctNeed
			left join  @cthd_NeedUpGiaVon ctNeed2 on ctNeed.RN  = ctNeed2.RN + 1 and ctNeed.NgayLapHoaDon = ctNeed2.NgayLapHoaDon
			and ctNeed.ID_HangHoa = ctNeed2.ID_HangHoa 
								and (ctNeed.ID_LoHang = ctNeed2.ID_LoHang or (ctNeed.ID_LoHang is null and ctNeed2.ID_LoHang is null))


			------ update GiaVonDauKy cho hdCurrent: dựa vào giá vốn này để tính GV cho các hóa đơn tiếp theo ----
			update ctNeed 
				set ctNeed.GiaVonDauKy = isnull(gvDK.GiaVonDauKy,0)				
			from @cthd_NeedUpGiaVon ctNeed
			left join #tblGVDauKy gvDK on ctNeed.ID_HangHoa = gvDK.ID_HangHoa 
				and (ctNeed.ID_LoHang = gvDK.ID_LoHang or (ctNeed.ID_LoHang is null and gvDK.ID_LoHang is null)) 
			where ctNeed.IDHoaDon = @IDHoaDonInput

			--select * from @cthd_NeedUpGiaVon ORDER BY  ID_HangHoa, ID_LoHang, RN

		
    		   			
    		DECLARE @IDHoaDon UNIQUEIDENTIFIER, @IDHoaDonGoc UNIQUEIDENTIFIER, @IDHoaDonCu UNIQUEIDENTIFIER;
    		DECLARE @MaHoaDon NVARCHAR(MAX), @LoaiHoaDon INT, @ChoThanhToan bit
    		DECLARE @IDChiTietHoaDon UNIQUEIDENTIFIER;
    		DECLARE @SoLuong FLOAT, @DonGia FLOAT, @ChietKhau FLOAT, @ThanhTien FLOAT;
    		DECLARE @TongTienHang float, @TongChiPhi float, @TongGiamGia FLOAT;
    		DECLARE @TyLeChuyenDoi FLOAT;
    		DECLARE @TonKho FLOAT;
    		DECLARE @IDHangHoa UNIQUEIDENTIFIER, @IDDonViQuiDoi UNIQUEIDENTIFIER, @IDLoHang UNIQUEIDENTIFIER;
    		DECLARE @IDChiNhanhThemMoi UNIQUEIDENTIFIER,  @IDCheckIn UNIQUEIDENTIFIER;
    		DECLARE @YeuCau NVARCHAR(MAX);
    		DECLARE @RN INT;
			DECLARE @GiaVonCu FLOAT, @GiaVon FLOAT, @GiaVonNhan FLOAT;
    		DECLARE @GiaVonMoi FLOAT, @GiaVonCuUpdate FLOAT;
    		DECLARE @GiaVonHoaDonBan FLOAT;
    		DECLARE @TongTienHangDemo FLOAT, @SoLuongDemo FLOAT, @ThanhTienDemo FLOAT,@ChietKhauDemo FLOAT;
			declare @ngayLapHDGoc datetime

				------ duyet cthdNeed update (order by ngaylaphoadon, idhanghoa) ----

				declare @cthdCurrent table (ID_HangHoa UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER,
					SoLuong float, DonGia float, ChietKhau float, GiaVon float,
					TyLeChuyenDoi float, TongTienHang float, TongChiPhi float, TongGiamGia float)

    		DECLARE CS_GiaVon CURSOR SCROLL LOCAL FOR 
    			SELECT IDHoaDon, IDHoaDonGoc, IdHoaDonTruocDo, MaHoaDon, LoaiHoaDon, ChoThanhToan,
					ID_ChiTietHoaDon, SoLuong, DonGia, TongTienHang, TongChiPhi,
				ChietKhau,ThanhTien, TongGiamGia, TyLeChuyenDoi, TonKho,
    			GiaVonDauKy, ID_HangHoa, IDDonViQuiDoi, ID_LoHang, ID_ChiNhanhThemMoi, ID_CheckIn, YeuCau, GiaVon, GiaVonNhan , RN
    			FROM @cthd_NeedUpGiaVon 
				WHERE LaHangHoa = 1 
				ORDER BY  ID_HangHoa, ID_LoHang, RN  ----- chạy lần lượt theo idHangHoa, IdLoHang (ngaylaphd min)
    		OPEN CS_GiaVon
    		FETCH FIRST FROM CS_GiaVon 
    			INTO @IDHoaDon, @IDHoaDonGoc, @IDHoaDonCu,  @MaHoaDon, @LoaiHoaDon, @ChoThanhToan,
				@IDChiTietHoaDon, @SoLuong, @DonGia, 
				@TongTienHang, @TongChiPhi, @ChietKhau,@ThanhTien, @TongGiamGia, @TyLeChuyenDoi, @TonKho,
    			@GiaVonCu, @IDHangHoa, @IDDonViQuiDoi, @IDLoHang, @IDChiNhanhThemMoi, @IDCheckIn, @YeuCau, @GiaVon, @GiaVonNhan, @RN
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
						-------- RN = 1: hd đầu tiên của hàng hóa, giữ nguyên GiaVonCu ban đầu (tức giavondauky) ---								
							if @RN > 1 set @GiaVonCu = @GiaVonCuUpdate
    				
							if @LoaiHoaDon in (4,13,7,10,6,18)
								begin
									---- nếu hd bị hủy/tạm lưu: get giavon cũ trước đó ---
									if @ChoThanhToan is null or @ChoThanhToan ='1' set @GiaVonMoi = @GiaVonCu
									else
										begin ----- begin ChoThanhToan = 0 (hd chưa hủy )--
												----- get cthd current is cursor ----
												insert into @cthdCurrent
												select ID_HangHoa, ID_LoHang, SoLuong, DonGia, ChietKhau, GiaVon, 
													TyLeChuyenDoi, TongTienHang, TongChiPhi, TongGiamGia
												FROM @cthd_NeedUpGiaVon cthd
    											WHERE cthd.IDHoaDon = @IDHoaDon AND cthd.ID_HangHoa = @IDHangHoa 
													AND (cthd.ID_LoHang = @IDLoHang or @IDLoHang is null and cthd.ID_LoHang is null)

												if @LoaiHoaDon in (4,13) ---- 4.nhaphang, 13.nhaphangkhachthua
													begin
														SELECT @TongTienHangDemo = SUM(cthd.SoLuong * (cthd.DonGia -  cthd.ChietKhau)), 
															@SoLuongDemo = SUM(cthd.SoLuong * cthd.TyLeChuyenDoi) 
    													FROM @cthdCurrent cthd
    													 group by cthd.ID_HangHoa, cthd.ID_LoHang

															IF(@SoLuongDemo + @TonKho > 0 AND @TonKho > 0)
    														BEGIN
    															IF(@TongTienHang != 0)
    															BEGIN
																	------ giavon: tinh sau khi tru giam gia hoadon + phi ship---   										
																	SET	@GiaVonMoi = (@GiaVonCu * @TonKho +
																		------ trừ giamgiaHD + chiphiVC ----
																		(@TongTienHangDemo * ( 1 - @TongGiamGia/@TongTienHang + @TongChiPhi/@TongTienHang)))
																		/(@SoLuongDemo + @TonKho)																													
							
    															END
    															ELSE
    															BEGIN
    																SET	@GiaVonMoi = ((@GiaVonCu * @TonKho) + @TongTienHangDemo)/(@SoLuongDemo + @TonKho);
    															END
							
    														END
    														ELSE
    														BEGIN	
																------ Tonkho dauky = 0 ----
    															IF(@TongTienHang != 0)
    															BEGIN
																	------ (thanh tien sau giamgia + chi phi VC)/ soluong
							
    																if @SoLuongDemo > 0
																		SET	@GiaVonMoi = (@TongTienHangDemo * (1 - @TongGiamGia / @TongTienHang) 
																						+ (@TongTienHangDemo * @TongChiPhi/@TongTienHang)
																						)/ @SoLuongDemo
																	else
																		 SET @GiaVonMoi = @GiaVonCu
								
    															END
    															ELSE
    															BEGIN
																	if @SoLuongDemo > 0
    																	SET	@GiaVonMoi = @TongTienHangDemo/@SoLuongDemo;
																	else
																		 SET @GiaVonMoi = @GiaVonCu
    															END
							
    														END
													end

												if @LoaiHoaDon = 7 --- trahang NCC
													begin
														SELECT @TongTienHangDemo = 
															SUM(cthd.SoLuong * cthd.DonGia * ( 1- cthd.TongGiamGia/iif(cthd.TongTienHang=0,1,cthd.TongTienHang))) ,
															@SoLuongDemo = SUM(cthd.SoLuong * cthd.TyLeChuyenDoi) 
    													FROM @cthdCurrent cthd   										
    													GROUP BY cthd.ID_HangHoa, cthd.ID_LoHang
    													IF(@TonKho - @SoLuongDemo > 0)
    													BEGIN										
    														SET	@GiaVonMoi = ((@GiaVonCu * @TonKho) - @TongTienHangDemo)/(@TonKho - @SoLuongDemo);												
    													END
    													ELSE
    													BEGIN
    														SET @GiaVonMoi = @GiaVonCu;
    													END
													end

												if @LoaiHoaDon = 10 ---- dieuchuyen --
													BEGIN
													SELECT @TongTienHangDemo = SUM(cthd.ChietKhau * cthd.DonGia), 
														@SoLuongDemo = SUM(cthd.ChietKhau * cthd.TyLeChuyenDoi) 
    												FROM @cthdCurrent cthd   								
    												GROUP BY cthd.ID_HangHoa, cthd.ID_LoHang 
									
    											IF(@YeuCau = '1' OR (@YeuCau = '4' AND @IDChiNhanhThemMoi != @IDCheckIn))
    											BEGIN
    												SET @GiaVonMoi = @GiaVonCu;
    											END
    											ELSE IF (@YeuCau = '4' AND @IDChiNhanhThemMoi = @IDCheckIn)
    											BEGIN
    												IF(@TonKho + @SoLuongDemo > 0 AND @TonKho > 0)
    												BEGIN
    													SET @GiaVonMoi = (@GiaVonCu * @TonKho + @TongTienHangDemo)/(@TonKho + @SoLuongDemo);
    												END
    												ELSE
    												BEGIN
    														IF(@SoLuongDemo = 0)
    														BEGIN
    															SET @GiaVonMoi = @GiaVonCu;
    														END
    														ELSE
    														BEGIN
    														SET @GiaVonMoi = @TongTienHangDemo/@SoLuongDemo;
    														END
    												END
    											END
											end

												if @LoaiHoaDon = 6 ---- khachang trahang
												begin
														SELECT @SoLuongDemo = SUM(cthd.SoLuong * cthd.TyLeChuyenDoi) 
    													FROM @cthdCurrent cthd    									
    													GROUP BY cthd.ID_HangHoa, cthd.ID_LoHang

    													IF(@IDHoaDonGoc IS NOT NULL)
    													BEGIN
    														SET @GiaVonHoaDonBan = -1;
															set @ngayLapHDGoc = (select top 1 NgayLapHoaDon from @tblCTMuaGoc where ID_HDMuaGoc = @IDHoaDonGoc)
																------ get giavon hangban tại thời điểm mua ---
																select 
																	@GiaVonHoaDonBan = GiaVon
																from
																(
																	select 
																		ctg.NgayLapHoaDon,
																		ctg.GiaVon,
																		ROW_NUMBER() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon desc) as RN
																	from @tblCTMuaGoc ctg
																	where ctg.LoaiHoaDon not in (3,19,25,29)
																		and ctg.NgayLapHoaDon <= @ngayLapHDGoc
																		and ctg.ID_HangHoa = @IDHangHoa
																		and (ctg.ID_LoHang = @IDLoHang or ctg.ID_LoHang is null and @IDLoHang is null)
																)tblg where Rn = 1

    						
    														IF(@GiaVonHoaDonBan = - 1) ----- tại thời điểm mua: chưa có giá vốn
    														BEGIN    				
    															set @GiaVonHoaDonBan = 0						
    														END
    														IF(@TonKho + @SoLuongDemo > 0 AND @TonKho > 0)
    														BEGIN    							
    															SET @GiaVonMoi = (@GiaVonCu * @TonKho + @GiaVonHoaDonBan * @SoLuongDemo) / (@TonKho + @SoLuongDemo);
    														END
    														ELSE
    														BEGIN
    															SET @GiaVonMoi = @GiaVonHoaDonBan;
    														END
    													END
    													ELSE
    													BEGIN
															----- nếu trả nhanh (không liên quan đến hóa đơn/gdv nào) ----
    														SET @GiaVonMoi = @GiaVonCu;
    													END
												end

												if @LoaiHoaDon = 18 --- phieu Dieuchinh Giavon
												begin		
													------ nếu phiếu điều chỉnh: gán = GiaVon luôn (không cần quy đổi nữa, vì đã quy đổi rồi) --
    													SELECT @GiaVonMoi = GiaVon
														FROM @cthdCurrent 		

												end

												if @GiaVonMoi is null set @GiaVonMoi = @GiaVonCu

												------ xoa hết @cthdCurrent để insert lại ---
												delete from @cthdCurrent
										end ----- end ChoThanhToan									
								end
							else
								begin ---- LoaiHoaDon #
									SET @GiaVonMoi = @GiaVonCu;
								end  								
					
					-------- update again GiaVonNew for ctNeed -----
					------select @RN, @IDHoaDon as idHoadon,@IDHoaDonCu as idhoadoncu, @GiaVonMoi as gvmoi, @GiaVonCuUpdate as gvcu
    				IF(@LoaiHoaDon = 10 AND @YeuCau = '4' AND @IDCheckIn = @IDChiNhanhThemMoi) ----- nhanhang ----
    				BEGIN --- 	------ nhanhang: update GiaVonNhan ----
						----- nếu hàng thuộc cùng hóa đơn,
						---- giá vốn mới lấy giống với giá vốn của cùng hàng hóa đó sau lần cập nhật đầu tiên ---
						if @IDHoaDon = @IDHoaDonCu set @GiaVonMoi = @GiaVonCuUpdate		

    					UPDATE @cthd_NeedUpGiaVon SET GiaVonNhan = @GiaVonMoi
							WHERE ID_ChiTietHoaDon = @IDChiTietHoaDon;    			
    				END
    				ELSE
    				BEGIN ------ các hdConLai (#nhanhang): update GiaVon  ---		
						if @IDHoaDon = @IDHoaDonCu set @GiaVonMoi = @GiaVonCuUpdate
    					UPDATE @cthd_NeedUpGiaVon SET GiaVon = @GiaVonMoi
						WHERE ID_ChiTietHoaDon = @IDChiTietHoaDon;  
    				END

					------ gán lại GV cũ = giá vốn mới vừa update: để tính lại GiaVon cho các HD tiếp theo----
					set @GiaVonCuUpdate = @GiaVonMoi
										
    			FETCH NEXT FROM CS_GiaVon INTO @IDHoaDon, @IDHoaDonGoc, @IDHoaDonCu, @MaHoaDon, @LoaiHoaDon, @ChoThanhToan,
				@IDChiTietHoaDon, @SoLuong, @DonGia, 
				@TongTienHang, @TongChiPhi, @ChietKhau, @ThanhTien, @TongGiamGia, @TyLeChuyenDoi, @TonKho,
    			@GiaVonCu, @IDHangHoa, @IDDonViQuiDoi, @IDLoHang, @IDChiNhanhThemMoi, @IDCheckIn, @YeuCau,  @GiaVon, @GiaVonNhan, @RN
    		END
    		CLOSE CS_GiaVon
    		DEALLOCATE CS_GiaVon	

			

			------- update again GiaVonDauKy for cthdNeed: used to tính lại giá trị chênh lệch của phiếu Điều chỉnh GV ----	
			update cthd set cthd.GiaVonDauKy =  isnull(ct.GiaVon,0) * cthd.TyLeChuyenDoi		
			from @cthd_NeedUpGiaVon cthd 
			left join @cthd_NeedUpGiaVon ct
				on ct.ID_HangHoa = cthd.ID_HangHoa 
							and (cthd.ID_LoHang = ct.ID_LoHang or (cthd.ID_LoHang is null and ct.ID_LoHang is null))									
							and cthd.RN = ct.RN + 1
			where cthd.RN > 1 ---- chỉ cập nhật gvDauKy cho Rn bắt đầu từ 2 --
		
				 
			

    		---- =========Update BH_HoaDon_ChiTiet ===========
		
			UPDATE hdct
    		SET hdct.GiaVon = gvNew.GiaVon * gvNew.TyLeChuyenDoi,
    			hdct.GiaVon_NhanChuyenHang = iif(gvNew.LoaiHoaDon not in (8,9), gvNew.GiaVonNhan * gvNew.TyLeChuyenDoi, hdct.GiaVon_NhanChuyenHang),
				------ phieuKiemKe: update ThanhToan = GiaVonLech ThanhToan ----
				hdct.ThanhToan = iif(gvNew.LoaiHoaDon = 9,hdct.SoLuong * gvNew.GiaVon * gvNew.TyLeChuyenDoi, hdct.ThanhToan),
				------ phieu XuatKho: update ThanhTien = GiaTriXuat ----
				hdct.ThanhTien = iif(gvNew.LoaiHoaDon = 8, hdct.SoLuong * gvNew.GiaVon * gvNew.TyLeChuyenDoi, hdct.ThanhTien)			
    		FROM BH_HoaDon_ChiTiet hdct
    		JOIN @cthd_NeedUpGiaVon AS gvNew ON hdct.ID = gvNew.ID_ChiTietHoaDon   
			WHERE gvNew.LoaiHoaDon !=18 

    
    		---- ==== update to Phieu DieuChinhGiaVon === ----
			--- DonGia = giavon truoc khi điều chỉnh: không cần quy đổi nữa, vì gvdk đã tính theo đơn vị quy đổi ----
			--- PTChietKhau: giá vốn lêch tăng ---
			--- TienChietKhau: giá vốn lệch giảm --- 
    		UPDATE hdct
    		SET hdct.DonGia = gvNew.GiaVonDauKy, 
    			hdct.PTChietKhau = CASE WHEN hdct.GiaVon - gvNew.GiaVonDauKy > 0 THEN hdct.GiaVon - gvNew.GiaVonDauKy ELSE 0 END,
    			hdct.TienChietKhau = CASE WHEN hdct.GiaVon - gvNew.GiaVonDauKy > 0 THEN 0 ELSE gvNew.GiaVonDauKy - hdct.GiaVon END
    		FROM BH_HoaDon_ChiTiet AS hdct
    		INNER JOIN @cthd_NeedUpGiaVon AS gvNew
    		ON hdct.ID = gvNew.ID_ChiTietHoaDon
    		WHERE gvNew.LoaiHoaDon = 18 
		


    
				------ Update GiaVon DichVu = sum GiaVon (ThanhPhan_DinhLuong) ------	
    		UPDATE hdct
    			SET hdct.GiaVon = iif(hdct.SoLuong = 0,0, isnull(gvDLuong.GiaVonDinhLuong,0) / hdct.SoLuong)
    		FROM BH_HoaDon_ChiTiet hdct
    		JOIN
    				(SELECT ct.ID_ChiTietDinhLuong,
    					SUM(ct.GiaVon * ct.SoLuong) AS GiaVonDinhLuong
    				FROM @cthd_NeedUpGiaVon ct
					where ct.LoaiHoaDon in (1,25,6)
					and ct.ID_ChiTietDinhLuong is not null
					and ct.ChatLieu!='5' --- khonglay dinhluong bi huy ----
    				GROUP BY ID_ChiTietDinhLuong
					) gvDLuong   			
    		ON hdct.ID = gvDLuong.ID_ChiTietDinhLuong

			
    		--=========END Update BH_HoaDon_ChiTiet

    		------ Update DM_GiaVon------
			------  get giavonHienTai (last GiaVon in cthdNeed) && update to DM_GiaVon ---					
			select distinct
				gvCurrent.ID_HangHoa,
				gvCurrent.ID_LoHang,
				gvCurrent.GiaVon * qd.TyLeChuyenDoi as GiaVon,
				qd.ID_DonViQuiDoi
			into #gvHienTai			
			from
			(
			select 
				ctNeed.ID_HangHoa,
				ctNeed.ID_LoHang,
				iif(ctNeed.ID_CheckIn = ctNeed.ID_ChiNhanhThemMoi,ctNeed.GiaVonNhan, ctNeed.GiaVon) as GiaVon,
				ROW_NUMBER() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon desc) as RN
			from @cthd_NeedUpGiaVon ctNeed
			) gvCurrent
			join @tblQuyDoi qd on gvCurrent.ID_HangHoa = qd.ID_HangHoa 
				and (gvCurrent.ID_LoHang = qd.ID_LoHang or gvCurrent.ID_LoHang is null and qd.ID_LoHang is null)
			where RN = 1

		
			------ only update this chinhanh ---
    		UPDATE gv
    			SET gv.GiaVon = ISNULL(gvHienTai.GiaVon, 0)
    		FROM DM_GiaVon gv
    		join #gvHienTai gvHienTai on gv.ID_DonViQuiDoi = gvHienTai.ID_DonViQuiDoi
				and (gv.ID_LoHang = gvHienTai.ID_LoHang or gvHienTai.ID_LoHang is null and gv.ID_LoHang is null)
			where gv.ID_DonVi = @IDChiNhanh
			
			----- insert to DM_GiaVon if not exists ----
			INSERT INTO DM_GiaVon (ID, ID_DonVi, ID_DonViQuiDoi, ID_LoHang, GiaVon)
			select 
				newID(),
				@IDChiNhanh,
				gvHienTai.ID_DonViQuiDoi,
				gvHienTai.ID_LoHang,
				gvHienTai.GiaVon
			from #gvHienTai gvHienTai
			where not exists (
				select id from DM_GiaVon gv
				where gv.ID_DonViQuiDoi = gvHienTai.ID_DonViQuiDoi
				and (gv.ID_LoHang = gvHienTai.ID_LoHang or gvHienTai.ID_LoHang is null and gv.ID_LoHang is null)
				and gv.ID_DonVi = @IDChiNhanh
			)	  
    		
			delete from BH_HoaDon_ChiTiet where ID_HoaDon = @IDHoaDonInput and ChatLieu='5'

		
			drop table #tblTonLuyKe
			drop table #tblGVDauKy
			drop table #gvHienTai
			drop table #ctTraHang

    		END --- end beginOut
    		
END");

			Sql(@"ALTER PROCEDURE [dbo].[HDTraHang_InsertTPDinhLuong]
    @ID_HoaDon [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    	--- get infor hdTra --> used to update TonLuyKe
    	declare @ID_HoaDonGoc uniqueidentifier, @ID_DonVi uniqueidentifier, @NgayLapHoaDon datetime
    	select @ID_HoaDonGoc = ID_HoaDon, @ID_DonVi = ID_DonVi, @NgayLapHoaDon= NgayLapHoaDon
    	from BH_HoaDon where ID= @ID_HoaDon
    
    	if @ID_HoaDonGoc is not null
    	begin		
    			---- get dluong at hdgoc
    			select ct.ID_ChiTietDinhLuong, ct.ID_DonViQuiDoi, ct.ID_LoHang, 
    					ct.GiaVon, ct.GiaVon_NhanChuyenHang,
    					ct.TonLuyKe, ct.TonLuyKe_NhanChuyenHang,
    				iif(ctDV.SoLuong = 0,0,ct.SoLuong/ctDV.SoLuong) as SoLuongMacDinh,
    				ct.TenHangHoaThayThe
    			into #tmpCTMua
    			from BH_HoaDon_ChiTiet ct		
    			left join (
    				---- get dv parent
    				select dlCha.ID_ChiTietDinhLuong, dlCha.SoLuong, dlCha.ID
    				from BH_HoaDon_ChiTiet dlCha
    				where dlCha.ID_HoaDon= @ID_HoaDonGoc
    				and (dlCha.ID_ChiTietDinhLuong is not null and dlCha.ID =dlCha.ID_ChiTietDinhLuong)
    			) ctDV on ct.ID_ChiTietDinhLuong = ctDV.ID_ChiTietDinhLuong
    			where ct.ID_HoaDon= @ID_HoaDonGoc 
    			and ct.ID_ChiTietDinhLuong is not null and ct.ID!=ct.ID_ChiTietDinhLuong
    
    			---- get ct hdTra
    			select ct.ID, ct.ID_ChiTietGoiDV, ct.SoLuong, ct.ChatLieu -- chatlieu:1.tra hd, 2.tra gdv
    			into #ctTra
    			from BH_HoaDon_ChiTiet ct where ct.ID_HoaDon= @ID_HoaDon
    
    			declare @CTTra_ID uniqueidentifier, @CTTra_IDChiTietGDV uniqueidentifier, @CTTra_SoLuong float, @CTTra_ChatLieu nvarchar(max)
    			declare _cur cursor
    			for
    			select ID, ID_ChiTietGoiDV, SoLuong, ChatLieu
    			from #ctTra
    			open _cur
    			fetch next from _cur
    			into @CTTra_ID, @CTTra_IDChiTietGDV, @CTTra_SoLuong, @CTTra_ChatLieu
    			while @@FETCH_STATUS = 0
    			begin
    
    				declare @countDLuong int = (select count (*) from #tmpCTMua where  ID_ChiTietDinhLuong= @CTTra_IDChiTietGDV)
    				if @countDLuong > 0
    				begin
    					---- insert dinhluong if has at ctmua
    					update BH_HoaDon_ChiTiet set ID_ChiTietDinhLuong= @CTTra_ID where ID= @CTTra_ID
    
    					insert into BH_HoaDon_ChiTiet (ID, ID_HoaDon,ID_ChiTietDinhLuong, SoThuTu, 
    											ID_DonViQuiDoi, ID_LoHang, SoLuong, DonGia, GiaVon, ThanhTien, ThanhToan, 
    										PTChietKhau, TienChietKhau, GiaVon_NhanChuyenHang,  PTChiPhi, TienChiPhi, TienThue, An_Hien, 
    											TonLuyKe, TonLuyKe_NhanChuyenHang, ChatLieu, TenHangHoaThayThe)		
    					select NEWID(), @ID_HoaDon, @CTTra_ID, 0, ID_DonViQuiDoi, ID_LoHang, SoLuongMacDinh * @CTTra_SoLuong, 0, GiaVon, 0, 0,
    								0,0, GiaVon_NhanChuyenHang,0,0,0,0,0,0, @CTTra_ChatLieu, TenHangHoaThayThe
    					from #tmpCTMua where ID_ChiTietDinhLuong= @CTTra_IDChiTietGDV
    				end		
    				fetch next from _cur into @CTTra_ID, @CTTra_IDChiTietGDV, @CTTra_SoLuong, @CTTra_ChatLieu
    			end
    			close _cur
    			deallocate _cur
    
			
    	end	
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDatHang_ChiTiet]
    @Text_Search [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN

	set nocount on;

	declare @tblLoai table(LoaiHang int)
	insert into @tblLoai select name from dbo.splitstring(@LoaiHangHoa)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	declare @tblCTHD table (
		NgayLapHoaDon datetime,
		MaHoaDon nvarchar(max),
		LoaiHoaDon int,
		ID_DonVi uniqueidentifier,
		ID_PhieuTiepNhan uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID_NhanVien uniqueidentifier,
		TongTienHang float,
		TongGiamGia	float,
		KhuyeMai_GiamGia float,
		ChoThanhToan bit,
		ID uniqueidentifier,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier,
		ID_LoHang uniqueidentifier,
		ID_ChiTietGoiDV	uniqueidentifier,
		ID_ChiTietDinhLuong uniqueidentifier,
		ID_ParentCombo uniqueidentifier,
		SoLuong float,
		DonGia float,
		GiaVonfloat float,
		TienChietKhau float,
		TienChiPhi float,
		ThanhTien float,
		ThanhToan float,
		GhiChu nvarchar(max),
		ChatLieu nvarchar(max),
		LoaiThoiGianBH int,
		ThoiGianBaoHanh float,
		TenHangHoaThayThe nvarchar(max),
		TienThue float,	
		GiamGiaHD float,
		GiamTruThanhToanBaoHiem float,
		GiaVon float,
		TienVon float
		)

	insert into @tblCTHD
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, '3'

	---- get cthd da xuly
	select ctxl.ID_ChiTietGoiDV,
		sum(ctxl.SoLuong) as SoLuongNhan,
		sum(ctxl.ThanhTien) as GiaTriNhan
	into #tblXuLy
	from BH_HoaDon_ChiTiet ctxl
	join BH_HoaDon hdxl on ctxl.ID_HoaDon= hdxl.ID
	where hdxl.LoaiHoaDon in (1,25)
	and hdxl.ChoThanhToan= 0
	and (ctxl.ID_ChiTietDinhLuong = ctxl.ID or ctxl.ID_ChiTietDinhLuong is null)			 
	and (ctxl.ID_ParentCombo is null or ctxl.ID_ParentCombo= ctxl.ID)	
	and exists (
	select ct.ID from @tblCTHD ct where ctxl.ID_ChiTietGoiDV= ct.ID
	)
	group by ctxl.ID_ChiTietGoiDV


	select *
	from
	(
		select ct.MaHoaDon, ct.NgayLapHoaDon, 
			ct.SoLuong as SoLuongDat, 
			ct.DonGia,
			ct.TienThue as TongTienThue,
			(ct.TienChietKhau * ct.SoLuong) as TongChietKhau,
			ct.ThanhTien as TongTienHang,
			ct.GiamGiaHD,
			ct.ThanhTien - ct.GiamGiaHD as GiaTriDat,
			isnull(xl.SoLuongNhan,0) as SoLuongNhan,
			dt.MaDoiTuong,  
			dt.TenDoiTuong as TenKhachHang,
			qd.MaHangHoa,
			qd.TenDonViTinh,
			hh.TenHangHoa,
			qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			CONCAT(hh.TenHangHoa, qd.ThuocTinhGiaTri) as TenHangHoaFull,
			lo.MaLoHang as TenLoHang,
			nv.TenNhanVien,
			ct.GhiChu,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa
		from @tblCTHD ct
		left join #tblXuLy xl on ct.ID= xl.ID_ChiTietGoiDV 
		left join DM_DoiTuong dt on ct.ID_DoiTuong= dt.ID
		left join NS_NhanVien nv on ct.ID_NhanVien= nv.ID
		left join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
		left join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
		left join DM_LoHang lo on ct.ID_LoHang= lo.ID
		left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID	
		where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)			 
				and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)		
		and hh.TheoDoi like @TheoDoi
		and qd.Xoa like @TrangThai
		and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID))	
		AND
			((select count(Name) from @tblSearchString b where 
    				hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or lo.MaLoHang like '%' +b.Name +'%' 
    				or qd.MaHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    				or qd.TenDonViTinh like '%'+b.Name+'%'					
    				or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
					or dt.TenDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
					or nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
					or ct.MaHoaDon like '%'+b.Name+'%'
					)=@count or @count=0)
	)tbl where tbl.LoaiHangHoa in (select LoaiHang from @tblLoai)
	order by tbl.NgayLapHoaDon desc    
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDatHang_TongHop]
    @Text_Search [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	set nocount on;
	declare @tblLoai table(LoaiHang int)
	insert into @tblLoai select name from dbo.splitstring(@LoaiHangHoa)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	declare @tblCTHD table (
		NgayLapHoaDon datetime,
		MaHoaDon nvarchar(max),
		LoaiHoaDon int,
		ID_DonVi uniqueidentifier,
		ID_PhieuTiepNhan uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID_NhanVien uniqueidentifier,
		TongTienHang float,
		TongGiamGia	float,
		KhuyeMai_GiamGia float,
		ChoThanhToan bit,
		ID uniqueidentifier,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier,
		ID_LoHang uniqueidentifier,
		ID_ChiTietGoiDV	uniqueidentifier,
		ID_ChiTietDinhLuong uniqueidentifier,
		ID_ParentCombo uniqueidentifier,
		SoLuong float,
		DonGia float,
		GiaVonfloat float,
		TienChietKhau float,
		TienChiPhi float,
		ThanhTien float,
		ThanhToan float,
		GhiChu nvarchar(max),
		ChatLieu nvarchar(max),
		LoaiThoiGianBH int,
		ThoiGianBaoHanh float,
		TenHangHoaThayThe nvarchar(max),
		TienThue float,	
		GiamGiaHD float,
		GiamTruThanhToanBaoHiem float,
		GiaVon float,
		TienVon float
		)

	insert into @tblCTHD
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, '3'

	---- get cthd da xuly
	select ctxl.ID_ChiTietGoiDV,
		sum(ctxl.SoLuong) as SoLuongNhan,sum(ctxl.ThanhTien) as GiaTriNhan
	into #tblXuLy
	from BH_HoaDon_ChiTiet ctxl
	join BH_HoaDon hdxl on ctxl.ID_HoaDon= hdxl.ID
	where hdxl.LoaiHoaDon in (1,25)
	and hdxl.ChoThanhToan= 0
	and (ctxl.ID_ChiTietDinhLuong = ctxl.ID or ctxl.ID_ChiTietDinhLuong is null)			 
	and (ctxl.ID_ParentCombo is null or ctxl.ID_ParentCombo= ctxl.ID)	
	and exists (
	select ct.ID from @tblCTHD ct where ctxl.ID_ChiTietGoiDV= ct.ID
	)
	group by ctxl.ID_ChiTietGoiDV

	select *
	from
	(
	select 
		tblDH.ID_DonViQuiDoi,
		tblDH.ID_LoHang,
		qd.MaHangHoa,
		lo.MaLoHang,
		hh.TenHangHoa,
		concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
		qd.TenDonViTinh,
		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
		tblDH.SoLuongDat,
		tblDH.ThanhTien,
		tblDH.GiamGiaHD,
		tblDH.TongChietKhau,
		tblDH.TongTienThue,
		tblDH.ThanhTien - tblDH.GiamGiaHD as GiaTriDat,
		tblDH.SoLuongNhan,
		isnull(nhh.TenNhomHangHoa,N'Nhóm mặc định') as TenNhomHangHoa,
		iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa
	from(
	select ct.ID_DonViQuiDoi, ct.ID_LoHang, 
		sum(ct.SoLuong) as SoLuongDat, 
		sum(ct.SoLuong * ct.TienChietKhau) as TongChietKhau,
		sum(ct.TienThue) as TongTienThue,
		sum(ct.ThanhTien) as ThanhTien,
		sum(ct.GiamGiaHD) as GiamGiaHD,
		sum(isnull(xl.SoLuongNhan,0)) as SoLuongNhan
	from @tblCTHD ct
	left join #tblXuLy xl on ct.ID= xl.ID_ChiTietGoiDV 
	where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)			 
			and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)	
	group by ct.ID_DonViQuiDoi, ct.ID_LoHang
	)tblDH
	join DonViQuiDoi qd on tblDH.ID_DonViQuiDoi = qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	left join DM_LoHang lo on tblDH.ID_LoHang= lo.ID	
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
	where hh.TheoDoi like @TheoDoi	  
	and qd.Xoa like @TrangThai
	and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)		  		)
    	AND
		((select count(Name) from @tblSearchString b where 
    			hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or lo.MaLoHang like '%' +b.Name +'%' 
    			or qd.MaHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    				or qd.TenDonViTinh like '%'+b.Name+'%'					
    
	or qd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
	) tbl
	where tbl.LoaiHangHoa in (select LoaiHang from @tblLoai)
	order by tbl.TenHangHoa desc 
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_HoaDonNhapHang]
    @TextSearch [nvarchar](max),
    @LoaiHoaDon varchar(50), ---- dùng chung cho nhập hàng + trả hàng nhập + nhập kho nội bộ
    @IDChiNhanhs [nvarchar](max),
	@IDNhanViens [nvarchar](max) ='',
    @FromDate [datetime],
    @ToDate [datetime],
    @TrangThais [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int],
    @ColumnSort [nvarchar](max),
    @SortBy [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	declare @tblChiNhanh table (ID varchar(40))
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs)

		declare @tblNhanVien table (ID varchar(40))   
		if isnull(@IDNhanViens,'')!=''
				insert into @tblNhanVien
    		select Name from dbo.splitstring(@IDNhanViens)
		else
			set @IDNhanViens =''

		declare @tblLoaiHD table (Loai int)
    	insert into @tblLoaiHD
    	select Name from dbo.splitstring(@LoaiHoaDon)
    
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch)


    	select hdQuy.*,
		hdQuy.PhaiThanhToan - hdQuy.KhachDaTra as ConNo1
		into #tmpNhapHang
    	from
    	(	
    	select hd.id, hd.ID_HoaDon, hd.MaHoaDon, hd.LoaiHoaDon, hd.DienGiai, hd.PhaiThanhToan, hd.ChoThanhToan,
    	hd.NgayLapHoaDon, hd.ID_NhanVien, hd.ID_BangGia, hd.TongTienHang, hd.TongChietKhau, hd.TongGiamGia, hd.TongChiPhi,
    	hd.TongTienThue, hd.TongThanhToan, hd.ID_DoiTuong, 
		ctHD.ThanhTienChuaCK,
		ctHD.GiamGiaCT,	

		iif(@LoaiHoaDon='7', -isnull(quy.TienThu,0),  isnull(quy.TienThu,0))  as KhachDaTra,
		iif(@LoaiHoaDon='7', 0,  isnull(quy.DaChi_BenVCKhac,0))  as DaChi_BenVCKhac,

		isnull(quy.TienMat,0) as TienMat,
		isnull(quy.ChuyenKhoan,0) as ChuyenKhoan,
		isnull(quy.TienATM,0) as TienATM,
		isnull(quy.TienDatCoc,0) as TienDatCoc,
		hd.NguoiTao, hd.NguoiTao as NguoiTaoHD,
    	dv.TenDonVi,hd.ID_DonVi,
		isnull(hd.PTThueHoaDon,0) as PTThueHoaDon,
    	isnull(dv.SoDienThoai,'') as DienThoaiChiNhanh,
    	isnull(dv.DiaChi,'') as DiaChiChiNhanh,
		iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',4,dt.LoaiDoiTuong), dt.LoaiDoiTuong) as LoaiDoiTuong,
		---- nhapnoibo: lay nhacc/nhanvien chung 1 cot
		iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',nv.MaNhanVien,dt.MaDoiTuong), dt.MaDoiTuong) as MaDoiTuong,
    	iif(hd.LoaiHoaDon = 13, iif(hd.ID_DoiTuong='00000000-0000-0000-0000-000000000002',nv.TenNhanVien,dt.TenDoiTuong), dt.TenDoiTuong) as TenDoiTuong,		
    	isnull(dt.DienThoai,'') as DienThoai,
    	isnull(dt.TenDoiTuong_KhongDau,'') as TenDoiTuong_KhongDau,
    	isnull(nv.MaNhanVien,'') as MaNhanVien,	
    	isnull(nv.TenNhanVien,'') as TenNhanVien,	
    	isnull(nv.TenNhanVienKhongDau,'') as TenNhanVienKhongDau,

		cpvc.ID_NhaCungCap,
		ncc.MaDoiTuong as MaNCCVanChuyen,
		ncc.TenDoiTuong as TenNCCVanChuyen,
		ncc.TenDoiTuong_KhongDau as TenNCCVanChuyen_KhongDau,

		hd.YeuCau,
		case hd.LoaiHoaDon
			when 4 then N'Nhập hàng'
			when 13 then N'Nhập kho nội bộ'
			when 14 then N'Nhập hàng khách thừa'
			when 31 then N'Đặt hàng nhà cung cấp'
		else 'Nhập hàng' end as strLoaiHoaDon,
		case hd.ChoThanhToan			
				when 1 then N'Tạm lưu' 
				when 0 then 
					case hd.YeuCau
						when '1' then  N'Đã lưu' 
						when '2' then  N'Đang xử lý' 
						when '3' then  N'Hoàn thành' 
						when '4' then  N'Đã hủy' 
						else iif(hd.LoaiHoaDon = 31, N'Đã lưu' , N'Đã nhập hàng') end
				else  N'Đã hủy'
				end as TrangThai,
			iif(hd.ChoThanhToan is null, '4',
			case hd.ChoThanhToan			
				when 1 then N'1' 
				when 0 then 
					case hd.YeuCau
						when '1' then '1'
						when '2' then '2'
						when '3' then '3'
						when '4' then '4'
						else '0' end
				else '4' end) as TrangThaiHD				    	
    	from BH_HoaDon hd
    	join DM_DonVi dv on hd.ID_DonVi= dv.ID
		left join BH_HoaDon_ChiPhi cpvc on hd.ID= cpvc.ID_HoaDon
		left join DM_DoiTuong ncc on cpvc.ID_NhaCungCap = ncc.ID
    	left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
		left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID
		left join (
			select 
				ct.ID_HoaDon,
				sum(ct.SoLuong * ct.TienChietKhau) as GiamGiaCT,			
				sum(ct.SoLuong * ct.DonGia) as ThanhTienChuaCK
			from BH_HoaDon_ChiTiet ct
    		join BH_HoaDon hd on ct.ID_HoaDon= hd.ID   			
    		where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    		and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    		and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
			group by ct.ID_HoaDon
		) ctHD on ctHD.ID_HoaDon = hd.ID
    	left join
    	(
				select 
					tblTongChi.ID_HoaDonLienQuan,
					sum(tblTongChi.TienThu) as TienThu,
					sum(tblTongChi.TienMat) as TienMat,
					sum(tblTongChi.TienATM) as TienATM,				
					sum(tblTongChi.ChuyenKhoan) as ChuyenKhoan,
					sum(tblTongChi.TienDatCoc) as TienDatCoc,
					sum(tblTongChi.KhachDaTra1) as KhachDaTra1,
					sum(tblTongChi.DaChi_BenVCKhac) as DaChi_BenVCKhac
				from
				(
						---- thong tin thanhtoan hoadon
    					select a.ID_HoaDonLienQuan, 
							sum(TienThu) as TienThu,
							sum(a.TienMat) as TienMat,
							sum(a.TienATM) as TienATM,				
							sum(a.ChuyenKhoan) as ChuyenKhoan,
							sum(a.TienDatCoc) as TienDatCoc,
							sum(a.KhachDaTra1) as KhachDaTra1,
							0 as DaChi_BenVCKhac
    					from(
					
    					select qct.ID_HoaDonLienQuan,   
							iif(qct.HinhThucThanhToan =1, qct.TienThu, 0) as TienMat,
							iif(qct.HinhThucThanhToan = 2, qct.TienThu,0) as TienATM,
							iif(qct.HinhThucThanhToan = 3, qct.TienThu,0) as ChuyenKhoan,
							iif(qct.HinhThucThanhToan = 6, qct.TienThu,0) as TienDatCoc,
							iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu) as TienThu,
							iif(qct.ID_DoiTuong = hd.ID_DoiTuong, iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu),0) as KhachDaTra1							
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID 
    					where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    					and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    					and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    					and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
    					) a group by a.ID_HoaDonLienQuan


						union all
						---- chi cho ben vckhac
						select chiVC.ID_HoaDonLienQuan, 
							0 as TienMat,
							0 as TienATM,
							0 as ChuyenKhoan,
							0 as TienDatCoc,
							0 as TienThu,
							0 as KhachDaTra1,
							sum(chiVC.TienThu) as DaChi_BenVCKhac
    					from(
					    	select qct.ID_HoaDonLienQuan,   						
								iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu) as TienThu
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID 
						join BH_HoaDon_ChiPhi cp on hd.ID = cp.ID_HoaDon and qct.ID_DoiTuong = cp.ID_NhaCungCap 
    					where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    					and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    					and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    					and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
    					) chiVC group by chiVC.ID_HoaDonLienQuan
					

						Union all
						---- get TongChi from PO: chi get hdXuly first
    					select 
							ID,
							TienThu,
							TienMat, 
							TienATM,
							ChuyenKhoan,
							TienDatCoc,
							0 as KhachDaTra1,
							0 as DaChi_BenVCKhac
						from
						(	
								Select 
										ROW_NUMBER() OVER(PARTITION BY ID_HoaDonLienQuan ORDER BY NgayLapHoaDon ASC) AS isFirst,						
    									d.ID,
										ID_HoaDonLienQuan,
										d.NgayLapHoaDon,
    									sum(d.TienMat) as TienMat,
    									SUM(ISNULL(d.TienATM, 0)) as TienATM,
    									SUM(ISNULL(d.TienCK, 0)) as ChuyenKhoan,
										SUM(ISNULL(d.TienDoiDiem, 0)) as TienDoiDiem,
										sum(d.ThuTuThe) as ThuTuThe,
    									sum(d.TienThu) as TienThu,
										sum(d.TienDatCoc) as TienDatCoc
									
    								FROM
    								(									
											select hd.ID, hd.NgayLapHoaDon,
												qct.ID_HoaDonLienQuan,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
												iif(qhd.LoaiHoaDon = 12, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
												iif(qhd.LoaiHoaDon = 12, qct.TienThu, -qct.TienThu) as TienThu,
												iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc
											from Quy_HoaDon_ChiTiet qct
											join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
											join BH_HoaDon hdd on hdd.ID= qct.ID_HoaDonLienQuan
											left join BH_HoaDon hd on hd.ID_HoaDon= hdd.ID
											where hdd.LoaiHoaDon = 31	
											and hd.ChoThanhToan = 0
											and (qhd.TrangThai= 1 Or qhd.TrangThai is null)
    								) d group by d.ID,d.NgayLapHoaDon,ID_HoaDonLienQuan						
						) thuDH
						where isFirst= 1
					)tblTongChi
					group by tblTongChi.ID_HoaDonLienQuan
    	) quy on hd.id = quy.ID_HoaDonLienQuan
    	where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
		and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    	and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
		and (@IDNhanViens ='' or exists (select  ID from @tblNhanVien nv where hd.ID_NhanVien = nv.ID))
    ) hdQuy
    where 
    exists (select ID from dbo.splitstring(@TrangThais) tt where hdQuy.TrangThaiHD= tt.Name)	
    	and
    	((select count(Name) from @tblSearch b where     			
    		hdQuy.MaHoaDon like '%'+b.Name+'%'
    		or hdQuy.NguoiTao like '%'+b.Name+'%'				
    		or hdQuy.TenNhanVien like '%'+b.Name+'%'
    		or hdQuy.TenNhanVienKhongDau like '%'+b.Name+'%'
    		or hdQuy.DienGiai like '%'+b.Name+'%'
    		or hdQuy.MaDoiTuong like '%'+b.Name+'%'		
    		or hdQuy.TenDoiTuong like '%'+b.Name+'%'
    		or hdQuy.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		or hdQuy.DienThoai like '%'+b.Name+'%'		
			or hdQuy.MaNCCVanChuyen like '%'+b.Name+'%'
    		or hdQuy.TenNCCVanChuyen like '%'+b.Name+'%'		
			or hdQuy.TenNCCVanChuyen_KhongDau like '%'+b.Name+'%'		
    		)=@count or @count=0)	
  


			----- get list ID of top 10
			declare @tblID TblID
			insert into @tblID
			select distinct ID from #tmpNhapHang

		
		
			------ only get congno of top 10			
			if @LoaiHoaDon = '7'
			begin
				declare @tblCongNoTH table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, HDDoi_PhaiThanhToan float, PhaiTraKhach float)
				insert into @tblCongNoTH
				exec TinhCongNo_HDTra @tblID, 7

				;with data_cte
				as
				(
				select tView.*,
					cn.MaHoaDonGoc,
					cn.LoaiHoaDonGoc,
					isnull(cn.HDDoi_PhaiThanhToan,0) as TongTienHDDoiTra,
					iif(tView.ID_HoaDon is null,tView.PhaiThanhToan, tView.PhaiThanhToan - isnull(cn.PhaiTraKhach,0)) as TongTienHDTra, --- muontruong: PhaiTraKhach (sau khi butru congno hdGoc & hdDoi)
					iif(tView.ID_HoaDon is null,tView.PhaiThanhToan - tView.KhachDaTra, tView.PhaiThanhToan - isnull(cn.PhaiTraKhach,0) - tView.KhachDaTra) as ConNo
				from #tmpNhapHang tView
				left join @tblCongNoTH cn on tView.ID = cn.ID
				),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
					sum(ThanhTienChuaCK) as SumThanhTienChuaCK,
					sum(GiamGiaCT) as SumGiamGiaCT,
    				sum(TongTienHang) as SumTongTienHang,
    				sum(TongGiamGia) as SumTongGiamGia,
					sum(TienMat) as SumTienMat,	
					sum(TienATM) as SumPOS,	
					sum(ChuyenKhoan) as SumChuyenKhoan,	
					sum(TienDatCoc) as SumTienCoc,	
    				sum(KhachDaTra) as SumDaThanhToan,				
    				sum(TongChiPhi) as SumTongChiPhi,
    				sum(PhaiThanhToan) as SumPhaiThanhToan,
    				sum(TongThanhToan) as SumTongThanhToan,				
    				sum(TongTienThue) as SumTongTienThue,
    				sum(ConNo) as SumConNo,
					sum(TongTienHDTra) as SumTongTienHDTra
    			from data_cte
    		),
			tView
			as
			(
    		select dt.*, cte.*	
    		from data_cte dt
    		cross join count_cte cte
    		order by 
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='ConNo' then ConNo1 end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='ConNo' then ConNo1 end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC			
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
			)
			select tView.* ,
				po.MaHoaDon as MaHoaDonGoc,
				isnull(po.LoaiHoaDon,0) as LoaiHoaDonGoc
			from tView tView
			left join BH_HoaDon po on tView.ID_HoaDon= po.ID		

			end
			else
			begin
				declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, TongTienHDTra float, KhachDaTra float)
				insert into @tblCongNo
				exec HD_GetBuTruTraHang @tblID, 7
			

				;with data_cte
				as
				(
				select tView.*,
					cn.MaHoaDonGoc,
					cn.LoaiHoaDonGoc,
					isnull(cn.TongTienHDTra,0) as TongTienHDTra, ---- if LoaiHD=7, TongTienDHTra = NCC CanTra
					tView.PhaiThanhToan - isnull(cn.TongTienHDTra,0) as gtriSauTra,
					iif(tView.ChoThanhToan is null,0, tView.PhaiThanhToan - isnull(cn.TongTienHDTra,0)- tView.KhachDaTra) as ConNo
				from #tmpNhapHang tView
				left join @tblCongNo cn on tView.ID = cn.ID
				),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
					sum(ThanhTienChuaCK) as SumThanhTienChuaCK,
					sum(GiamGiaCT) as SumGiamGiaCT,
    				sum(TongTienHang) as SumTongTienHang,
    				sum(TongGiamGia) as SumTongGiamGia,
					sum(TienMat) as SumTienMat,	
					sum(TienATM) as SumPOS,	
					sum(ChuyenKhoan) as SumChuyenKhoan,	
					sum(TienDatCoc) as SumTienCoc,	
    				sum(KhachDaTra) as SumDaThanhToan,				
    				sum(TongChiPhi) as SumTongChiPhi,
    				sum(PhaiThanhToan) as SumPhaiThanhToan,
    				sum(TongThanhToan) as SumTongThanhToan,				
    				sum(TongTienThue) as SumTongTienThue,
    				sum(ConNo) as SumConNo, --- 105889500
					sum(TongTienHDTra) as SumTongTienHDTra  ,
					sum(DaChi_BenVCKhac) as SumDaChi_BenVCKhac  
    			from data_cte
    		)
			,
			tView
			as
			(
    		select dt.*, cte.*	
    		from data_cte dt
    		cross join count_cte cte
    		order by 
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='ConNo' then ConNo end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='ConNo' then ConNo end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaHoaDon' then MaHoaDon end DESC,
    			case when @SortBy <>'ASC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end ASC,
    			case when @SortBy <>'DESC' then ''
    			when @ColumnSort='MaKhachHang' then dt.MaDoiTuong end DESC,
    			case when @SortBy <> 'ASC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end ASC,
    			case when @SortBy <> 'DESC' then 0
    			when @ColumnSort='TongTienHang' then TongTienHang end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='GiamGia' then TongGiamGia end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='PhaiThanhToan' then PhaiThanhToan end DESC,
    			case when @SortBy <>'ASC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end ASC,
    			case when @SortBy <>'DESC' then 0
    			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC			
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
			)
			select tView.* ,
				po.MaHoaDon as MaHoaDonGoc,
				isnull(po.LoaiHoaDon,0) as LoaiHoaDonGoc
			from tView tView
			left join BH_HoaDon po on tView.ID_HoaDon= po.ID		
			end
END");

			Sql(@"ALTER PROCEDURE [dbo].[Gara_GetListBaoGia]
    @IDChiNhanhs [nvarchar](max),
    @FromDate [nvarchar](14),
    @ToDate [nvarchar](14),
    @ID_PhieuSuaChua [nvarchar](max), --%%
	@IDXe uniqueidentifier = null,
    @TrangThais [nvarchar](20), -- 0,1
    @TextSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	if @FromDate = '2016-01-01' 
    		set @ToDate= (select format(DATEADD(day,1, max(NgayLapHoaDon)),'yyyy-MM-dd') from BH_HoaDon where LoaiHoaDon= 3)
    
    	declare @tblDonVi table (ID_DonVi uniqueidentifier)

		declare @totalRow int ;
    	
		if isnull(@IDChiNhanhs,'')=''
    	BEGIN
			INSERT INTO @tblDonVi
    		SELECT ID FROM DM_DonVi;
    	END
    	ELSE
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IDChiNhanhs) where Name!=''
    	END
    
    	declare @tbTrangThai table (GiaTri varchar(2))
    	insert into @tbTrangThai
    	select Name from dbo.splitstring(@TrangThais)
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);	
    
 
    			select *
				into #temp
    			from
    			(
    			select hd.ID,
					hd.ID_HoaDon,
					hd.ID_DonVi,
					hd.ID_DoiTuong,
					hd.ID_NhanVien,
					hd.ID_Xe,
					hd.NgayLapHoaDon,
					hd.MaHoaDon,
					hd.LoaiHoaDon,
					hd.ChoThanhToan,
					hd.TongTienHang,
					hd.TongGiamGia,
					hd.TongChietKhau,
					hd.TongTienThue,
					hd.TongChiPhi,
					hd.PhaiThanhToan,
					
					hd.YeuCau,
					hd.ID_PhieuTiepNhan,
					hd.ID_BangGia,
					hd.ID_BaoHiem,
					hd.TongThanhToan,
					hd.NguoiTao,
					hd.DienGiai,

					xe.BienSo,
					
    				tn.MaPhieuTiepNhan,
					isnull(hd.PTThueHoaDon,0) as PTThueHoaDon,
					isnull(tblQuy.KhachDaTra,0) as KhachDaTra,
					isnull(hd.PTThueBaoHiem,0) as PTThueBaoHiem,
					isnull(hd.TongTienThueBaoHiem,0) as TongTienThueBaoHiem,
					isnull(hd.SoVuBaoHiem,0) as SoVuBaoHiem,
					isnull(hd.KhauTruTheoVu,0) as KhauTruTheoVu,
					isnull(hd.TongTienBHDuyet,0) as TongTienBHDuyet,
					isnull(hd.PTGiamTruBoiThuong,0) as PTGiamTruBoiThuong,
					isnull(hd.GiamTruBoiThuong,0) as GiamTruBoiThuong,
					isnull(hd.BHThanhToanTruocThue,0) as BHThanhToanTruocThue,
					isnull(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem,
					iif(hd.ID_BaoHiem is null,'',tn.NguoiLienHeBH) as LienHeBaoHiem,
					iif(hd.ID_BaoHiem is null,'',tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,
					isnull(tblQuy.Khach_TienMat,0) as Khach_TienMat,
					isnull(tblQuy.Khach_TienPOS,0) as Khach_TienPOS,
					isnull(tblQuy.Khach_TienCK,0) as Khach_TienCK,
					isnull(tblQuy.Khach_TienDiem,0) as Khach_TienDiem,
					isnull(tblQuy.Khach_TheGiaTri,0) as Khach_TheGiaTri,
					isnull(tblQuy.Khach_TienCoc,0) as Khach_TienCoc,

    				dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, 
					dt.Email,
					dt.DiaChi,
					dt.MaSoThue,
					dt.TaiKhoanNganHang,
    				case hd.ChoThanhToan
    					when 0 then '0'
    					when 1 then '1'
    					else '2' end as TrangThai,
    				case hd.ChoThanhToan
    					when 0 
    						then 
    							case hd.YeuCau
    							when '1' then N'Đã duyệt'
    							when '2' then N'Đang xử lý'
    							when '3' then N'Hoàn thành'
    							end 
    					when 1 then N'Chờ duyệt'
    					else N'Đã hủy'
    					end as TrangThaiText
    			from BH_HoaDon hd
    			join Gara_PhieuTiepNhan tn on tn.ID= hd.ID_PhieuTiepNhan
				left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
    			left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
				left join(
				select 
    				b.ID,
    				SUM(ISNULL(b.KhachDaTra, 0)) as KhachDaTra,
					SUM(ISNULL(b.Khach_TienMat, 0)) as Khach_TienMat,
					SUM(ISNULL(b.Khach_TienPOS, 0)) as Khach_TienPOS,
					SUM(ISNULL(b.Khach_TienCK, 0)) as Khach_TienCK,
					SUM(ISNULL(b.Khach_TheGiaTri, 0)) as Khach_TheGiaTri,
					SUM(ISNULL(b.Khach_TienDiem, 0)) as Khach_TienDiem,
					SUM(ISNULL(b.Khach_TienCoc, 0)) as Khach_TienCoc
    			from
    			(
    					-- get infor PhieuThu from HDDatHang (HuyPhieuThu (qhd.TrangThai ='0')
    				Select 
    					hdd.ID,		
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=1, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienMat,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=2, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienPOS,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=3, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienCK,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=4, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TheGiaTri,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=5, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienDiem,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=6, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienCoc,
						iif(qhd.TrangThai='0',0, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu)) as KhachDaTra    							
    					from BH_HoaDon hdd
    				left join Quy_HoaDon_ChiTiet hdct on hdd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on hdct.ID_HoaDon = qhd.ID 				
    				where hdd.LoaiHoaDon = '3' and hdd.ChoThanhToan is not null
    					and hdd.NgayLapHoadon between @FromDate and  @ToDate
						and exists (select ID_DonVi from @tblDonVi dv where qhd.ID_DonVi = dv.ID_DonVi)
    
    				union all
    					-- get infor PhieuThu/Chi from HDXuLy
    				Select
    					hdd.ID,			
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=1, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienMat,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=2, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienPOS,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=3, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienCK,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=4, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TheGiaTri,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=5, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienDiem,
						iif(qhd.TrangThai='0',0, iif(hdct.HinhThucThanhToan=6, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu),0)) as Khach_TienCoc,
						iif(qhd.TrangThai='0' or bhhd.ChoThanhToan is null,0, iif(qhd.LoaiHoaDon = 11, hdct.TienThu, - hdct.TienThu)) as KhachDaTra       									
    				from BH_HoaDon bhhd
    				join BH_HoaDon hdd on (bhhd.ID_HoaDon = hdd.ID and hdd.ChoThanhToan = '0')
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on (hdct.ID_HoaDon = qhd.ID)
    				where hdd.LoaiHoaDon = '3' 
    					and bhhd.ChoThanhToan='0'
    					and bhhd.NgayLapHoadon between @FromDate and  @ToDate
						and exists (select ID_DonVi from @tblDonVi dv where hdd.ID_DonVi = dv.ID_DonVi)
    			) b
    			group by b.ID 
				) tblQuy on hd.ID= tblQuy.ID
    			where hd.LoaiHoaDon= 3
    			and exists (select ID_DonVi from @tblDonVi dv where tn.ID_DonVi = dv.ID_DonVi)
    			and hd.NgayLapHoaDon between @FromDate and  @ToDate
    			and hd.ID_PhieuTiepNhan like @ID_PhieuSuaChua
				and (@IDXe is null or hd.ID_Xe = @IDXe)
    			and
    				((select count(Name) from @tblSearch b where     			
    				hd.MaHoaDon like '%'+b.Name+'%'
    				or tn.MaPhieuTiepNhan like '%'+b.Name+'%'
    				or dt.MaDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoiTuong like '%'+b.Name+'%'	
    				or dt.DienThoai like '%'+b.Name+'%'				
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'			
    				or hd.NguoiTao like '%'+b.Name+'%'										
    				)=@count or @count=0)	
    			) a 
				where exists (select GiaTri from @tbTrangThai tt where a.TrangThai = tt.GiaTri)



	select @totalRow = count(ID) from #temp

	select dt.*,
		dt.NguoiTao as NguoiTaoHD,
		nv.TenNhanVien,
		@totalRow as TotalRow
	from #temp dt
	join NS_NhanVien nv on dt.ID_NhanVien= nv.ID 
	order by NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetInforHoaDon_ByID]
	@ID_HoaDon [nvarchar](max) 
AS
BEGIN

	SET NOCOUNT ON;

	declare @idDatHang uniqueidentifier 
	set @idDatHang=(select  top 1 hdd.ID
		from BH_HoaDon hd
		join BH_HoaDon hdd on hd.ID_HoaDon= hdd.ID
		where hd.ID = @ID_HoaDon and hdd.LoaiHoaDon= 3 and hdd.ChoThanhToan='0'
	)


		select tblDebit.*,
			cpVC.ID_NhaCungCap,

			xe.BienSo,
			tn.MaPhieuTiepNhan,
			tn.SoKmVao,
			tn.SoKmRa,

			cus.MaDoiTuong,		
			cus.DienThoai,
			cus.Email,
			cus.DiaChi, 
			cus.MaSoThue,
			cus.TaiKhoanNganHang,

			ISNULL(cus.TenDoiTuong,N'Khách lẻ')  as TenDoiTuong,
    		ISNULL(bg.TenGiaBan,N'Bảng giá chung') as TenBangGia,
    		ISNULL(nv.TenNhanVien,N'')  as TenNhanVien,
    		ISNULL(dv.TenDonVi,N'')  as TenDonVi,    

			ncc.MaDoiTuong as MaNCCVanChuyen,
			ncc.TenDoiTuong as TenNCCVanChuyen,
			ncc.MaDoiTuong as MaNCCVanChuyen,


			bh.TenDoiTuong as TenBaoHiem,
    		bh.MaDoiTuong as MaBaoHiem,
			isnull(bh.Email,'') as BH_Email,
    		isnull(bh.DiaChi,'') as BH_DiaChi,
    		isnull(bh.DienThoai,'') as DienThoaiBaoHiem,

			iif(tblDebit.ID_BaoHiem is null,'',tn.NguoiLienHeBH) as LienHeBaoHiem,
			iif(tblDebit.ID_BaoHiem is null,'',tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,

			---- get 2 trường này chỉ mục đích KhuyenMai thooi daay !!!---
			cus.NgaySinh_NgayTLap,
			case when cus.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' 
					else  ISNULL(cus.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as IDNhomDoiTuongs,


			iif(cpVC.ID_NhaCungCap = tblDebit.ID_DoiTuong,0,isnull(cpVC.DaChi_BenVCKhac,0)) as DaChi_BenVCKhac,
			iif(tblDebit.LoaiHoaDonGoc = 6, 
							--- neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = TongGiaTriTra					
							iif(tblDebit.LuyKeTraHang > 0, tblDebit.TongGiaTriTra, 
								---- neu LuyKeTrahang < 0, và > giátrị hóa đơn, thì giá trị bù trừ = giá trị hóa đơn
								---- ngược lại: giá trị bù trừ = abs (lũy kế)
								iif(abs(tblDebit.LuyKeTraHang) > tblDebit.TongThanhToan, tblDebit.TongThanhToan, abs(tblDebit.LuyKeTraHang))
								),
						---- neuhdGoc: tongtra > khachno: giá trị bù trừ = nợ còn lại
						iif(tblDebit.LuyKeTraHang > tblDebit.KhachNo, tblDebit.KhachNo,tblDebit.LuyKeTraHang)
						 ) as TongTienHDTra
		from
		(
		select tblLuyKe.*,		
				---- nếu hdGoc: get tổng giá trị trả của chính nó ----
				isnull(iif(tblLuyKe.LoaiHoaDonGoc = 3 or tblLuyKe.ID_HoaDon is null, tblLuyKe.TongGiaTriTra,					
					(select dbo.BuTruTraHang_HDDoi(ID_HoaDon,NgayLapHoaDon,ID_HoaDonGoc, LoaiHoaDonGoc))				
				),0) as LuyKeTraHang
		from
		(
			select 
				hd.*,				
				hdg.MaHoaDon as MaHoaDonGoc,
				hdg.LoaiHoaDon as LoaiHoaDonGoc,
				hdg.ID_HoaDon as ID_HoaDonGoc,
				ISNULL(allTra.TongGtriTra,0) as TongGiaTriTra,
				ISNULL(allTra.NoTraHang,0) as NoTraHang,

				isnull(sq.KhachDaTra,0) as KhachDaTra,
				isnull(sq.BaoHiemDaTra,0) as BaoHiemDaTra,
				isnull(sq.ThuDatHang,0) as ThuDatHang,

				ISNULL(hd.PhaiThanhToan,0) - ISNULL(sq.KhachDaTra,0) as KhachNo,
				isnull(sq.Khach_TienMat,0) + ISNULL(sq.BH_TienMat,0) as TienMat,
    			isnull(sq.Khach_TienPOS,0) + ISNULL(sq.BH_TienPOS,0) as TienATM,
    			isnull(sq.Khach_TienCK,0) + ISNULL(sq.BH_TienCK,0) as ChuyenKhoan,
    			isnull(sq.Khach_TienDiem,0) as TienDoiDiem,
    			isnull(sq.Khach_TheGiaTri,0) as ThuTuThe,		
				isnull(sq.Khach_TienCoc,0) + isnull(sq.BH_TienCoc,0) as TienDatCoc,
				isnull(sq.KhachDaTra,0) + isnull(sq.BaoHiemDaTra,0) as DaThanhToan
			from
			(
				select
						hd.ID,
						hd.ID_NhanVien,
						hd.ID_BangGia,
						hd.ID_HoaDon,
						hd.ID_DoiTuong,
						hd.ID_BaoHiem,
						hd.ID_CheckIn, 
						hd.ID_ViTri,
						hd.ID_Xe,
						hd.ID_KhuyenMai,
    					hd.LoaiHoaDon,
    					hd.MaHoaDon,
    					hd.NgayLapHoaDon,
    					hd.ID_PhieuTiepNhan, 
    					hd.TongTienHang,
    					hd.ChoThanhToan,
						hd.YeuCau,
						hd.SoVuBaoHiem,
						hd.DiemGiaoDich,
						hd.TongChietKhau,
						hd.ChiPhi_GhiChu,
					
						ISNULL(hd.KhuyeMai_GiamGia,0) as KhuyeMai_GiamGia,
    					ISNULL(hd.TongGiamGia,0) + ISNULL(hd.KhuyeMai_GiamGia, 0) as TongGiamGia, 
    					ISNULL(hd.PhaiThanhToan,0)   as PhaiThanhToan,
						ISNULL(hd.TongThanhToan,0)  as TongThanhToan,    				    						
						iif(hd.LoaiHoaDon=6 or hd.LoaiHoaDon = 4, isnull(hd.TongChiPhi,0) , isnull(hd.ChiPhi,0)) as TongChiPhi, 
						case when hd.NgayApDungGoiDV is null then '' else  convert(varchar(14), hd.NgayApDungGoiDV ,103) end  as NgayApDungGoiDV,
    					case when hd.HanSuDungGoiDV is null then '' else  convert(varchar(14), hd.HanSuDungGoiDV ,103) end as HanSuDungGoiDV,
						hd.NguoiTao as NguoiTaoHD,
						hd.NguoiTao,
						hd.NgaySua,
						hd.NgayTao,
    					hd.DienGiai,
    					hd.ID_DonVi,
    					hd.TongTienThue,
    					isnull(hd.TongTienBHDuyet,0) as TongTienBHDuyet, 
    					isnull(hd.PTThueHoaDon,0) as PTThueHoaDon, 
    					isnull(hd.PTThueBaoHiem,0) as PTThueBaoHiem, 
    					isnull(hd.TongTienThueBaoHiem,0) as TongTienThueBaoHiem, 
    					isnull(hd.KhauTruTheoVu,0) as KhauTruTheoVu, 
						isnull(hd.CongThucBaoHiem,0) as  CongThucBaoHiem,
						hd.GiamTruThanhToanBaoHiem as  GiamTruThanhToanBaoHiem,

    					isnull(hd.PTGiamTruBoiThuong,0) as PTGiamTruBoiThuong, 
    					isnull(hd.GiamTruBoiThuong,0) as GiamTruBoiThuong, 
    					isnull(hd.BHThanhToanTruocThue,0) as BHThanhToanTruocThue, 
    					isnull(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem, 
						isnull(hd.TongThueKhachHang,0) as  TongThueKhachHang,
    			
						case hd.ChoThanhToan
    						when 0 then '0'
    						when 1 then '1'
    						else '2' end as TrangThai,

						case hd.LoaiHoaDon
							when 3 then
								case hd.YeuCau
									when '1' then iif( hd.ID_PhieuTiepNhan is null, N'Phiếu tạm', iif(hd.ChoThanhToan='0',  N'Đã duyệt',N'Chờ duyệt'))
									when '2' then  N'Đang xử lý'
									when '3' then N'Hoàn thành'
								else N'Đã hủy' end
							else
								case hd.ChoThanhToan
									when 0 then N'Hoàn thành'
    								when 1 then N'Phiếu tạm'
    								else N'Đã hủy'
								end
						end as TrangThaiText 
						from BH_HoaDon hd where hd.ID= @ID_HoaDon
				)hd
				left join BH_HoaDon hdg on hd.ID_HoaDon = hdg.ID
				left join
				(
					------ all trahang of hdgoc ---
					select 					
						hdt.ID_HoaDon,					
						sum(hdt.PhaiThanhToan) as TongGtriTra,
						sum(hdt.PhaiThanhToan - isnull(chiHDTra.DaTraKhach,0)) as NoTraHang
					from BH_HoaDon hdt	
					left join
					(
						select 
							qct.ID_HoaDonLienQuan,
							sum(iif(qhd.LoaiHoaDon=12, qct.TienThu, -qct.TienThu)) as DaTraKhach
						from Quy_HoaDon_ChiTiet qct
						join Quy_HoaDon qhd on qct.ID_HoaDon= qct.ID_HoaDonLienQuan
						where qhd.TrangThai='0'					
						group by qct.ID_HoaDonLienQuan
					) chiHDTra on hdt.ID = chiHDTra.ID_HoaDonLienQuan
					where hdt.LoaiHoaDon= 6
					and hdt.ChoThanhToan='0'
					group by hdt.ID_HoaDon		
				) allTra on hd.ID= allTra.ID_HoaDon
				left join
				(
						------ thuchi all (hdThis + dathang)  ---
						select 
							tblUnion.ID_HoaDonLienQuan,
							sum(Khach_TienMat) as Khach_TienMat,
							sum(Khach_TienPOS) as Khach_TienPOS,
							sum(Khach_TienCK) as Khach_TienCK,
							sum(Khach_TheGiaTri) as Khach_TheGiaTri,
							sum(Khach_TienDiem) as Khach_TienDiem,
							sum(Khach_TienCoc) as Khach_TienCoc,
							sum(KhachDaTra) as KhachDaTra,
							sum(ThuDatHang) as ThuDatHang,
							sum(BH_TienMat) as BH_TienMat,
							sum(BH_TienPOS) as BH_TienPOS,
							sum(BH_TienCK) as BH_TienCK,
							sum(BH_TheGiaTri) as BH_TheGiaTri,
							sum(BH_TienDiem) as BH_TienDiem,
							sum(BH_TienCoc) as BH_TienCoc,
							sum(BaoHiemDaTra) as BaoHiemDaTra
						from
						(
								---- khach/baohiem datra (hdThis)---    			
								select 
									qct.ID_HoaDonLienQuan,
									---- LoaiDoiTuong: 1,2: dùng chung cho KH + NCC --
									sum(iif(dt.LoaiDoiTuong != 3,iif(qct.HinhThucThanhToan=1, qct.TienThu, 0),0)) as Khach_TienMat,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.HinhThucThanhToan=2, qct.TienThu, 0),0)) as Khach_TienPOS,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.HinhThucThanhToan=3, qct.TienThu, 0),0)) as Khach_TienCK,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.HinhThucThanhToan=4, qct.TienThu, 0),0)) as Khach_TheGiaTri,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.HinhThucThanhToan=5, qct.TienThu, 0),0)) as Khach_TienDiem,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.HinhThucThanhToan=6, qct.TienThu, 0),0)) as Khach_TienCoc,
									sum(iif(dt.LoaiDoiTuong!= 3,iif(qct.LoaiHoaDon=11, qct.TienThu, -qct.TienThu),0)) as KhachDaTra,
									0 as ThuDatHang,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=1, qct.TienThu, 0),0)) as BH_TienMat,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=3, qct.TienThu, 0),0)) as BH_TienPOS,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=3, qct.TienThu, 0),0)) as BH_TienCK,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=4, qct.TienThu, 0),0)) as BH_TheGiaTri,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=5, qct.TienThu, 0),0)) as BH_TienDiem,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.HinhThucThanhToan=6, qct.TienThu, 0),0)) as BH_TienCoc,
									sum(iif(dt.LoaiDoiTuong = 3,iif(qct.LoaiHoaDon=11, qct.TienThu, -qct.TienThu),0)) as BaoHiemDaTra								
								from
								(
								select 
									qct.ID_DoiTuong,
									qct.ID_HoaDonLienQuan,
									qct.HinhThucThanhToan,
									qct.TienThu,
									qhd.LoaiHoaDon
								from Quy_HoaDon_ChiTiet qct
    							join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    							where qhd.TrangThai= 1
    							and qct.ID_HoaDonLienQuan = @ID_HoaDon
								)qct
								join DM_DoiTuong dt on qct.ID_DoiTuong = dt.ID
								group by qct.ID_HoaDonLienQuan


								union all


							
								select 
									hdFirst.ID,
									thuDH.Khach_TienMat,
									thuDH.Khach_TienPOS,
									thuDH.Khach_TienCK,
									thuDH.Khach_TheGiaTri,
									thuDH.Khach_TienDiem,
									thuDH.Khach_TienCoc,
									thuDH.TienThu as KhachDaTra,
									thuDH.TienThu as ThuDatHang,

									0 as BH_TienMat,
									0 as BH_TienPOS,
									0 as BH_TienCK,
									0 as BH_TheGiaTri,
									0 as BH_TienDiem,
									0 as BH_TienCoc,
									0 as BaoHiemDaTra
								from
								(
									---- get all hdXuly from dathang  ---
								select 
									hdXuLy.ID,
									hdXuLy.ID_HoaDon,
									hdXuLy.NgayLapHoaDon,
									ROW_NUMBER() over (partition by hdXuLy.ID_HoaDon order by NgayLapHoaDon) as RN
								from BH_HoaDon hdXuLy
								where hdXuLy.ID_HoaDon= @idDatHang
								and hdXuLy.ChoThanhToan = 0							
								)hdFirst
								join
								(
								----2. get thuDathang ----
									select 
										qct.ID_HoaDonLienQuan as ID_DatHang,
										sum(iif(qct.HinhThucThanhToan=1, qct.TienThu, 0)) as Khach_TienMat,
										sum(iif(qct.HinhThucThanhToan=2, qct.TienThu, 0)) as Khach_TienPOS,
										sum(iif(qct.HinhThucThanhToan=3, qct.TienThu, 0)) as Khach_TienCK,
										sum(iif(qct.HinhThucThanhToan=4, qct.TienThu, 0)) as Khach_TheGiaTri,
										sum(iif(qct.HinhThucThanhToan=5, qct.TienThu, 0)) as Khach_TienDiem,
										sum(iif(qct.HinhThucThanhToan=6, qct.TienThu, 0)) as Khach_TienCoc,		
										sum(iif(qhd.LoaiHoaDon = 11,qct.TienThu, -qct.TienThu)) as TienThu	
									from Quy_HoaDon qhd								
									join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon = qhd.ID
									where qct.ID_HoaDonLienQuan = @idDatHang
									and (qhd.TrangThai= 1 Or qhd.TrangThai is null)		
									group by qct.ID_HoaDonLienQuan
								)thuDH on hdFirst.ID_HoaDon = thuDH.ID_DatHang
								where hdFirst.RN = 1
						)tblUnion
						group by tblUnion.ID_HoaDonLienQuan
				)sq on hd.ID= sq.ID_HoaDonLienQuan
		)tblLuyKe
		)tblDebit
		left join 
		(
			------ ben vc + chiphi vc
			select cp.ID_NhaCungCap,
				cp.ID_HoaDon,
				isnull(chiVC.TienThu,0)  as DaChi_BenVCKhac
			from BH_HoaDon_ChiPhi cp 
			left join
			(
				select 
					qct.ID_HoaDonLienQuan,   	
					qct.ID_DoiTuong,   
					sum(iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu)) as TienThu
				from Quy_HoaDon qhd
				join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon= qhd.ID
				where  qct.ID_HoaDonLienQuan = @ID_HoaDon
				and (qhd.TrangThai= 1 or qhd.TrangThai is null)				
				group by qct.ID_HoaDonLienQuan,  qct.ID_DoiTuong		
			) chiVC on chiVC.ID_DoiTuong = cp.ID_NhaCungCap 
			where  cp.ID_HoaDon = @ID_HoaDon
		) cpVC on tblDebit.ID = cpVC.ID_HoaDon
		left join Gara_PhieuTiepNhan tn on tblDebit.ID_PhieuTiepNhan = tn.ID
		left join Gara_DanhMucXe xe on tblDebit.ID_Xe = xe.ID
		left join DM_DoiTuong cus on tblDebit.ID_DoiTuong = cus.ID
		left join DM_DoiTuong ncc on cpVC.ID_NhaCungCap = ncc.ID
		left join DM_DoiTuong bh on tblDebit.ID_BaoHiem = bh.ID
		left join DM_GiaBan bg on tblDebit.ID_BangGia = bg.ID
		left join NS_NhanVien nv on tblDebit.ID_NhanVien = nv.ID
		left join DM_DonVi dv on tblDebit.ID_DonVi = dv.ID
END
");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDon_afterTraHang_DichVu]
 --declare  
 @IDChiNhanhs nvarchar(max) = 'd93b17ea-89b9-4ecf-b242-d03b8cde71de',
   @IDNhanViens nvarchar(max) = null,
   @DateFrom datetime = '2022-01-01',
   @DateTo datetime = null,
   @TextSearch nvarchar(max) = 'HDBL20071904528',
   @CurrentPage int =0,
   @PageSize int = 20
AS
BEGIN
set nocount on;

		if isnull(@CurrentPage,'') =''
			set @CurrentPage = 0
		if isnull(@PageSize,'') =''
		set @PageSize = 30

		if isnull(@DateFrom,'') =''
		begin	
			set @DateFrom = '2016-01-01'		
		end

		if isnull(@DateTo,'') =''
		begin		
			set @DateTo = DATEADD(day, 1, getdate())		
		end
		else
		set @DateTo = DATEADD(day, 1, @DateTo)

			DECLARE @tblChiNhanh table (ID uniqueidentifier primary key)
			if isnull(@IDChiNhanhs,'') !=''
				insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs)		
			else
				set @IDChiNhanhs =''

			DECLARE @tblSearch TABLE (Name [nvarchar](max))
			DECLARE @count int
			INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!=''
			select @count =  (Select count(*) from @tblSearch)

	; with data_cte
	as
	(
			SELECT 
				hd.ID,
				hd.MaHoaDon,
				hd.LoaiHoaDon,
				hd.NgayLapHoaDon,   						
				hd.ID_DoiTuong,	
				hd.ID_HoaDon,
				hd.ID_BangGia,
				hd.ID_NhanVien,
				hd.ID_DonVi,
				hd.ID_Xe,
				hd.ID_PhieuTiepNhan,
				hd.ID_BaoHiem,
				hd.NguoiTao,	
				hd.DienGiai,	
				hd.ChoThanhToan,
				dt.MaDoiTuong,
				dt.TenDoiTuong,
				xe.BienSo,
				iif(hd.TongThanhToan =0 or hd.TongThanhToan is null,  hd.PhaiThanhToan, hd.TongThanhToan) as TongThanhToan,
				ISNULL(hd.PhaiThanhToan, 0) as PhaiThanhToan,
				ISNULL(hd.TongTienHang, 0) as TongTienHang,
				ISNULL(hd.TongGiamGia, 0) as TongGiamGia,
				isnull(hd.TongChietKhau,0) as  TongChietKhau,
				ISNULL(hd.DiemGiaoDich, 0) as DiemGiaoDich,							
				ISNULL(hd.TongTienThue, 0) as TongTienThue,						
				isnull(hd.PTThueHoaDon,0) as  PTThueHoaDon,
				ISNULL(hd.TongThueKhachHang, 0) as TongThueKhachHang,	
				isnull(hd.TongTienThueBaoHiem,0) as  TongTienThueBaoHiem,
				isnull(hd.TongTienBHDuyet,0) as  TongTienBHDuyet,
				isnull(hd.SoVuBaoHiem,0) as  SoVuBaoHiem,
				isnull(hd.PTThueBaoHiem,0) as  PTThueBaoHiem,
				isnull(hd.KhauTruTheoVu,0) as  KhauTruTheoVu,
				isnull(hd.GiamTruBoiThuong,0) as  GiamTruBoiThuong,
				isnull(hd.PTGiamTruBoiThuong,0) as  PTGiamTruBoiThuong,
				isnull(hd.BHThanhToanTruocThue,0) as  BHThanhToanTruocThue,
				isnull(hd.PhaiThanhToanBaoHiem,0) as  PhaiThanhToanBaoHiem,
				tblConLai.SoLuongBan,
				tblConLai.SoLuongTra,
				tblConLai.SoLuongConLai
			FROM
			(
					select 
						tblUnion.ID,
						sum(tblUnion.SoLuongBan) as SoLuongBan,
						sum(isnull(tblUnion.SoLuongTra,0)) as SoLuongTra,
						sum(tblUnion.SoLuongBan) - sum(isnull(tblUnion.SoLuongTra,0)) as SoLuongConLai
					from
					(
							------ mua ----
							select 
								hd.ID,
								sum(ct.SoLuong) as SoLuongBan,
								0 as SoLuongTra
							from BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
							where hd.ChoThanhToan=0
							and hd.LoaiHoaDon in (1,25)
							and hd.NgayLapHoaDon between @DateFrom and @DateTo	
							and (@IDChiNhanhs ='' or exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID))
							and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID) ---- chi get hanghoa + dv
							group by hd.ID


							union all

						----- tra ----
							select hd.ID_HoaDon,
								0 as SoLuongBan,
								sum(ct.SoLuong) as SoLuongTra
							from BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon  
							where hd.ChoThanhToan = 0  
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID)													
							group by hd.ID_HoaDon

						---- todo check xuatkho hdsc ---

		
					) tblUnion
					group by tblUnion.ID
			) tblConLai 
			JOIN BH_HoaDon hd ON tblConLai.ID =	hd.ID	
			left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID 
			left join Gara_DanhMucXe xe on hd.ID_Xe = xe.ID		
			where tblConLai.SoLuongConLai >0
			and ((select count(Name) from @tblSearch b where     			
					hd.MaHoaDon like '%'+b.Name+'%'								
					or dt.MaDoiTuong like '%'+b.Name+'%'		
					or dt.TenDoiTuong like '%'+b.Name+'%'
					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
					or dt.DienThoai like '%'+b.Name+'%'		
					or xe.BienSo like '%'+b.Name+'%'			
					)=@count or @count=0)
					)
	, count_cte
	as
	(
		select count (ID) as TotalRow,
		ceiling(count (ID) / cast(@PageSize as float)) as TotalPage
		from data_cte
	),
	tView
	as
	(
	select *
	from data_cte
	cross join count_cte
	order by NgayLapHoaDon desc
	offset (@CurrentPage * @PageSize) rows
	fetch next @PageSize rows only
	)
	----- get row from- to
	select *
	into #tblView
	from tView



			------ thu baohiem ----
			declare @tblThuBaoHiem table (ID_HoaDonLienQuan uniqueidentifier primary key, BaoHiemDaTra float)
			insert into @tblThuBaoHiem
				select 
					hd.ID as ID_HoaDonLienQuan, 
					sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as BaoHiemDaTra			
				from #tblView hd				
				join Quy_HoaDon_ChiTiet qct on hd.ID= qct.ID_HoaDonLienQuan and hd.ID_BaoHiem = qct.ID_DoiTuong
				join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
				where (qhd.TrangThai is null or qhd.TrangThai = '1')		
				group by hd.ID

		
		select 
			cnLast.*,
			nv.TenNhanVien
		from
		(
		select 
				tblLast.*,
				iif(tblLast.ChoThanhToan is null,0, 
							----- hdDoi co congno < tongtra							
							tblLast.TongThanhToan 
								--- neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = TongGiaTriTra							
								- iif(tblLast.LoaiHoaDonGoc = 6, iif(tblLast.LuyKeTraHang > 0,  tblLast.TongGiaTriTra, 
									iif(abs(tblLast.LuyKeTraHang) > tblLast.TongThanhToan, tblLast.TongThanhToan, abs(tblLast.LuyKeTraHang))), tblLast.LuyKeTraHang)
								- tblLast.KhachDaTra ) as ConNo ---- ConNo = TongThanhToan - GtriBuTru
		from
		(
				select 
					tbl.*,
						isnull(iif(tbl.LoaiHoaDonGoc = 3 or tbl.ID_HoaDon is null,
						iif(tbl.KhachNo <= 0, 0, ---  khachtra thuatien --> công nợ âm
							case when tbl.TongGiaTriTra > tbl.KhachNo then tbl.KhachNo						
							else tbl.TongGiaTriTra end),
						(select dbo.BuTruTraHang_HDDoi(tbl.ID_HoaDon,tbl.NgayLapHoaDon,tbl.ID_HoaDonGoc, tbl.LoaiHoaDonGoc))				
					),0) as LuyKeTraHang	
				from
				(
					select hd.*	,
						hdgoc.ID as ID_HoaDonGoc,
						hdgoc.LoaiHoaDon as LoaiHoaDonGoc,
						hdgoc.MaHoaDon as MaHoaDonGoc,

						ISNULL(allTra.TongGtriTra,0) as TongGiaTriTra,	
						ISNULL(allTra.NoTraHang,0) as NoTraHang,
						isnull(sqHD.KhachDaTra,0) as KhachDaTra,
						hd.TongThanhToan- isnull(sqHD.KhachDaTra,0) as KhachNo
					from #tblView hd
					left join BH_HoaDon hdgoc on hd.ID_HoaDon= hdgoc.ID
					left join
					(
							select 
								tbUnion.ID_HoaDonLienQuan,
								sum(isnull(tbUnion.KhachDaTra,0)) as KhachDaTra
							from
							(
								------ thu hoadon -----
								select 
									qct.ID_HoaDonLienQuan,
									sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, - qct.TienThu)) as KhachDaTra
								from Quy_HoaDon qhd
								join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon= qhd.ID
								where qhd.TrangThai='1'
								and exists (select hd.ID from #tblView hd where qct.ID_HoaDonLienQuan = hd.ID and  hd.ID_DoiTuong = qct.ID_DoiTuong)
								group by qct.ID_HoaDonLienQuan

								union all

								------ thudathang ---
								select 
									hdFirst.ID,
									hdFirst.KhachDaTra
								from
								(
									select 
										hdxl.ID,
										thuDH.KhachDaTra,
										ROW_NUMBER() over (partition by hdxl.ID_HoaDon order by hdxl.NgayLapHoaDon) as RN
									from BH_HoaDon hdxl
									join 
									(
										select 
											qct.ID_HoaDonLienQuan,
											sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, - qct.TienThu)) as KhachDaTra
										from Quy_HoaDon qhd
										join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
										where qhd.TrangThai='1'
										and exists (select hd.ID from #tblView hd where qct.ID_HoaDonLienQuan = hd.ID_HoaDon and  hd.ID_DoiTuong = qct.ID_DoiTuong)
										group by qct.ID_HoaDonLienQuan
									) thuDH on thuDH.ID_HoaDonLienQuan = hdxl.ID_HoaDon
									where exists (select ID from #tblView hd where hdxl.ID_HoaDon = hd.ID_HoaDon)
									and hdxl.LoaiHoaDon in (1,25)
									and hdxl.ChoThanhToan='0'
								) hdFirst 
								where hdFirst.RN= 1
							) tbUnion group by tbUnion.ID_HoaDonLienQuan
					) sqHD on sqHD.ID_HoaDonLienQuan = hd.ID
				left join
					(
						------ all trahang of hdThis ---
						select 					
							hdt.ID_HoaDon,					
							sum(hdt.PhaiThanhToan) as TongGtriTra,
							sum(hdt.PhaiThanhToan - isnull(chiHDTra.DaTraKhach,0)) as NoTraHang
						from BH_HoaDon hdt	
						left join
						(
							select 
								qct.ID_HoaDonLienQuan,
								sum(iif(qhd.LoaiHoaDon=12, qct.TienThu, -qct.TienThu)) as DaTraKhach
							from Quy_HoaDon_ChiTiet qct
							join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
							where qhd.TrangThai='0'					
							group by qct.ID_HoaDonLienQuan
						) chiHDTra on hdt.ID = chiHDTra.ID_HoaDonLienQuan
						where hdt.LoaiHoaDon= 6
						and hdt.ChoThanhToan='0'
						group by hdt.ID_HoaDon		
					) allTra on allTra.ID_HoaDon = hd.ID
				) tbl
		)tblLast
		left join @tblThuBaoHiem thuBH on tblLast.ID= thuBH.ID_HoaDonLienQuan
		) cnLast
		left join NS_NhanVien nv on cnLast.ID_NhanVien= nv.ID
		drop table #tblView

END");

			Sql(@"ALTER PROCEDURE [dbo].[getListDanhSachHHImportKiemKe]
    @MaLoHangIP [nvarchar](max),
    @MaHangHoaIP [nvarchar](max),
    @ID_DonViIP [uniqueidentifier],
    @TimeIP [datetime]
AS
BEGIN

		set nocount on;

		select 
			hh.ID,
    		lh.ID as ID_LoHang,
			dvqd.ID as ID_DonViQuiDoi,
			dvqd.MaHangHoa,
    		hh.TenHangHoa,
    		hh.QuanLyTheoLoHang,
    		dvqd.TenDonViTinh,
    		dvqd.TyLeChuyenDoi,
    		dvqd.GiaNhap,
			lh.NgaySanXuat,
			lh.NgayHetHan,
			cast(0 as float) as TonKho, --- get tonkho + giavon at js (other function)
			cast(0 as float) as GiaVon,
			dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			Case when lh.ID is null then '' else lh.MaLoHang end as MaLoHang    				
		from
		(
		select 
			qd.ID,
			qd.ID_HangHoa,
			qd.MaHangHoa,
			qd.TenDonViTinh,
			qd.ThuocTinhGiaTri,
			qd.TyLeChuyenDoi,
			qd.GiaNhap			
		from DonViQuiDoi qd
		where rtrim(ltrim(qd.MaHangHoa)) =  @MaHangHoaIP
		and qd.Xoa='0'
		)dvqd
		join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    	left join DM_LoHang lh on lh.ID_HangHoa = hh.ID and lh.MaLoHang = @MaLoHangIP     	
    	where hh.TheoDoi = 1 		
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetTonKho_byIDQuyDois]
    @ID_ChiNhanh [uniqueidentifier],
	@IdHoaDonUpdate uniqueidentifier = null,
    @ToDate [datetime],
    @IDDonViQuyDois [nvarchar](max),
    @IDLoHangs [nvarchar](max)
AS
BEGIN
	 SET NOCOUNT ON;
    	declare @tblIDQuiDoi table (ID_DonViQuyDoi uniqueidentifier)
    	declare @tblIDLoHang table (ID_LoHang uniqueidentifier)
    
    	insert into @tblIDQuiDoi
    	select distinct Name from dbo.splitstring(@IDDonViQuyDois) 
    	insert into @tblIDLoHang
    	select distinct Name from dbo.splitstring(@IDLoHangs) where Name not like '%null%' and Name !=''

		if(isnull(@IdHoaDonUpdate,'00000000-0000-0000-0000-000000000000')!='00000000-0000-0000-0000-000000000000')
			set @ToDate = (select top 1 NgayLapHoaDon from BH_HoaDon where ID= @IdHoaDonUpdate)
		
		---- get tonluyke theo thoigian 
		SELECT 
    		ID_DonViQuiDoi,
    		ID_HangHoa, 		
    		ID_LoHang,		
    		IIF(LoaiHoaDon = 10 AND ID_CheckIn = ID_DonViInput, TonLuyKe_NhanChuyenHang, TonLuyKe) AS TonKho, 
    		IIF(LoaiHoaDon = 10 AND ID_CheckIn = ID_DonViInput, GiaVon_NhanChuyenHang, GiaVon) AS GiaVon
		into #tblTon
    	FROM (
    		SELECT tbltemp.*, ROW_NUMBER() OVER (PARTITION BY tbltemp.ID_HangHoa, tbltemp.ID_LoHang, 
			tbltemp.ID_DonViInput ORDER BY tbltemp.ThoiGian DESC) AS RN 
		
    	FROM (
				select 
						hd.LoaiHoaDon, 
						hd.ID_DonVi,
						qd.ID_HangHoa,
						ct.ID_DonViQuiDoi,
						hd.ID_CheckIn, 					
						@ID_ChiNhanh as ID_DonViInput, 
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh, 
						ct.TonLuyKe_NhanChuyenHang, ct.TonLuyKe) AS TonLuyKe,
    					ct.TonLuyKe_NhanChuyenHang,
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh, 
    					ct.GiaVon_NhanChuyenHang, 
    					ct.GiaVon)/ISNULL(qd.TyLeChuyenDoi,1) AS GiaVon,
    					ct.GiaVon_NhanChuyenHang, 
    					ct.ID_LoHang ,
    					IIF(hd.LoaiHoaDon = 10 AND hd.YeuCau = '4' AND hd.ID_CheckIn = @ID_ChiNhanh,
						hd.NgaySua, hd.NgayLapHoaDon) AS ThoiGian
				
				from 
					(
						--- get all dvquydoi by idhanghoa 
						select qdOut.ID, qdOut.TyLeChuyenDoi,  qdOut.ID_HangHoa
						from DonViQuiDoi qdOut
						where exists (
							select dvqd.ID_HangHoa
							from @tblIDQuiDoi qd
							join DonViQuiDoi dvqd on qd.ID_DonViQuyDoi= dvqd.ID
							where qdOut.ID_HangHoa = dvqd.ID_HangHoa
							)
					) qd
				join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
				join BH_HoaDon_ChiTiet ct on qd.ID = ct.ID_DonViQuiDoi
				join BH_HoaDon hd on ct.ID_HoaDon= hd.ID				
				where (hd.ID_DonVi= @ID_ChiNhanh or (hd.ID_CheckIn = @ID_ChiNhanh and hd.YeuCau = '4'))
				and hd.ChoThanhToan = 0 AND hd.LoaiHoaDon IN (1, 5, 7, 8, 4, 6, 9, 10,18,13)
				and (exists( select ID_LoHang from @tblIDLoHang lo2 where lo2.ID_LoHang= ct.ID_LoHang) Or hh.QuanLyTheoLoHang= 0)    
			) as tbltemp
    	WHERE tbltemp.ThoiGian < @ToDate) tblTonKhoTemp
    	WHERE tblTonKhoTemp.RN = 1;


		select TenHangHoa, 
			lo.MaLoHang,
			qd2.ID_HangHoa,
			qd.ID_DonViQuyDoi as ID_DonViQuiDoi,
			qd2.GiaNhap,
			lo.ID as ID_LoHang, 
			qd2.TyLeChuyenDoi,
			qd2.TenDonViTinh,
			isnull(tk.TonKho,0)/ iif(qd2.TyLeChuyenDoi=0 or qd2.TyLeChuyenDoi is null,1, qd2.TyLeChuyenDoi) as TonKho,
			isnull(tk.GiaVon,0) * iif(qd2.TyLeChuyenDoi=0 or qd2.TyLeChuyenDoi is null,1, qd2.TyLeChuyenDoi) as GiaVon
		from @tblIDQuiDoi qd 	
		join DonViQuiDoi qd2 on qd.ID_DonViQuyDoi= qd2.ID 
		join DM_HangHoa hh on hh.ID = qd2.ID_HangHoa
		left join DM_LoHang lo on hh.ID = lo.ID_HangHoa and hh.QuanLyTheoLoHang = 1   
		left join #tblTon tk on hh.ID = tk.ID_HangHoa 
		and (tk.ID_LoHang = lo.ID or hh.QuanLyTheoLoHang =0)
		where (exists( select ID_LoHang from @tblIDLoHang lo2 where lo2.ID_LoHang= lo.ID) Or hh.QuanLyTheoLoHang= 0)
		order by qd.ID_DonViQuyDoi, lo.ID

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetTPDinhLuong_ofCTHD]
    @ID_CTHD [uniqueidentifier],
	@LoaiHoaDon int null
AS
BEGIN
    SET NOCOUNT ON;

	if @LoaiHoaDon is null
	begin
		-- hoadonban
		select  MaHangHoa, TenHangHoa, ID_DonViQuiDoi, TenDonViTinh, SoLuong, ct.GiaVon, ct.ID_HoaDon,ID_ChiTietGoiDV, ct.ID_LoHang,
			ct.SoLuongDinhLuong_BanDau,
			hh.QuanLyTheoLoHang,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa ='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,--- used to check tonkho tpdl ---
			iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe,
    		case when ISNULL(QuyCach,0) = 0 then ISNULL(TyLeChuyenDoi,1) else ISNULL(QuyCach,0) * ISNULL(TyLeChuyenDoi,1) end as QuyCach,
    		qd.TenDonViTinh as DonViTinhQuyCach, ct.GhiChu	,
			ceiling(qd.GiaNhap) as GiaNhap, qd.GiaBan as GiaBanHH, lo.MaLoHang, lo.NgaySanXuat, lo.NgayHetHan ---- used to nhaphang tu hoadon
    	from BH_HoaDon_ChiTiet ct
    	Join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
		left join DM_LoHang lo on ct.ID_LoHang = lo.ID
    	where ID_ChiTietDinhLuong = @ID_CTHD and ct.ID != @ID_CTHD
		and ct.SoLuong > 0
		and (ct.ChatLieu is null or ct.ChatLieu !='5')
	end
	else
		-- hdxuatkho co Tpdinhluong
		begin	
		
			-- get thongtin hang xuatkho
			declare @ID_DonViQuiDoi uniqueidentifier, @ID_LoHang uniqueidentifier,  @ID_HoaDonXK uniqueidentifier
			select @ID_DonViQuiDoi= ID_DonViQuiDoi, @ID_LoHang= ID_LoHang, @ID_HoaDonXK = ct.ID_HoaDon
			from BH_HoaDon_ChiTiet ct 
			where ct.ID = @ID_CTHD 


			-- chi get dinhluong thuoc phieu xuatkho nay
			select ct.ID_ChiTietDinhLuong,ct.ID_ChiTietGoiDV, ct.ID_DonViQuiDoi, ct.ID_LoHang,
				ct.SoLuong, ct.DonGia, ct.GiaVon, ct.ThanhTien,ct.ID_HoaDon, ct.GhiChu, ct.ChatLieu,
				qd.MaHangHoa, qd.TenDonViTinh,
				lo.MaLoHang,lo.NgaySanXuat, lo.NgayHetHan,
				hh.TenHangHoa,
				qd.GiaBan,
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				qd.TenDonViTinh,
				qd.ID_HangHoa,
				hh.QuanLyTheoLoHang,
				hh.LaHangHoa,
				hh.DichVuTheoGio,
				hh.DuocTichDiem,
				ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
				CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
				hh.ID_NhomHang as ID_NhomHangHoa, 
				ISNULL(hh.GhiChu,'') as GhiChuHH,
				iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa ='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa, 
				iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe
			from BH_HoaDon_ChiTiet ct
			Join DonViQuiDoi qd on ct.ID_ChiTietDinhLuong = qd.ID
    		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
			left join DM_LoHang lo on ct.ID_LoHang= lo.ID
			where ct.ID_DonViQuiDoi= @ID_DonViQuiDoi 
			and ct.ID_HoaDon = @ID_HoaDonXK
			and ((ct.ID_LoHang = @ID_LoHang) or (ct.ID_LoHang is null and @ID_LoHang is null))	
			and (ct.ChatLieu is null or ct.ChatLieu !='5')
		end		
		
END");

			Sql(@"ALTER PROCEDURE [dbo].[ListHangHoaTheKho]
    @ID_HangHoa [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier]
AS
BEGIN
	SET NOCOUNT ON;
	SELECT ID_HoaDon, 
		MaHoaDon, 
		NgayLapHoaDon,
		LoaiHoaDon, 
		ID_DonVi,
		ID_CheckIn,		
		sum(table1.SoLuong) as SoLuong,
		max(table1.GiaVon) as GiaVon,
		ROUND(max(table1.TonKho),3) as TonKho,
		ROUND(sum(sum(table1.SoLuong)) over ( order by NgayLapHoaDon ),3) as LuyKeTonKho,
	case table1.LoaiHoaDon
			when 10 then case when table1.ID_CheckIn = @IDChiNhanh then N'Nhận chuyển hàng' else N'Chuyển hàng' end
			when 1 then N'Bán hàng'
			when 2 then N'Bảo hành'
			when 4 then N'Nhập hàng'
			when 6 then N'Khách trả hàng'
			when 7 then N'Trả hàng nhập'
			when 8 then N'Xuất kho'
			when 9 then N'Kiểm hàng'
			when 13 then N'Nhập nội bộ'
			when 14 then N'Nhập hàng thừa'
			when 18 then N'Điều chỉnh giá vốn'			
		end as LoaiChungTu
	FROM
	(
		SELECT hd.ID as ID_HoaDon, hd.MaHoaDon, hd.LoaiHoaDon, 
		CASE WHEN hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4' and hd.LoaiHoaDon = 10 THEN hd.NgaySua ELSE hd.NgayLapHoaDon END as NgayLapHoaDon,
		bhct.ThanhTien * dvqd.TyLeChuyenDoi as ThanhTien,
		bhct.TienChietKhau * dvqd.TyLeChuyenDoi TienChietKhau, 
		dvqd.TyLeChuyenDoi,
		hd.YeuCau, 
		hd.ID_CheckIn,
		hd.ID_DonVi,
		hh.QuanLyTheoLoHang,
		dvqd.LaDonViChuan, 
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.TonLuyKe, bhct.TonLuyKe_NhanChuyenHang) as TonKho,
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.GiaVon / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi),bhct.GiaVon_NhanChuyenHang / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi)) as GiaVon,	
		bhct.SoThuTu,
		(case hd.LoaiHoaDon
			when 9 then bhct.SoLuong ---- Số lượng lệch = SLThucTe - TonKhoDB        (-) Giảm  (+): Tăng
			when 10 then
				case when hd.ID_CheckIn= @IDChiNhanh and hd.YeuCau = '4' then bhct.TienChietKhau  ---- da nhanhang
				else iif(hd.YeuCau = '4',- bhct.TienChietKhau,- bhct.SoLuong) end 
			--- xuat
			when 1 then - bhct.SoLuong
			when 2 then - bhct.SoLuong
			when 7 then - bhct.SoLuong
			when 8 then - bhct.SoLuong
			--- conlai: nhap
			else bhct.SoLuong end
		) * dvqd.TyLeChuyenDoi as SoLuong
		
	FROM BH_HoaDon hd
	LEFT JOIN BH_HoaDon_ChiTiet bhct on hd.ID = bhct.ID_HoaDon
	LEFT JOIN DonViQuiDoi dvqd on bhct.ID_DonViQuiDoi = dvqd.ID
	LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
	WHERE hd.LoaiHoaDon not in (3 ,19, 25,29,31)
	and hd.ChoThanhToan = 0
	and (bhct.ChatLieu is null or bhct.ChatLieu not in ('2','5') ) --- ChatLieu = 2: tra GDV, 5. cthd da xoa
	and  hh.ID = @ID_HangHoa 
	and ((hd.ID_DonVi = @IDChiNhanh and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null)) or (hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4'))
	)  table1
	group by ID_HoaDon, MaHoaDon, NgayLapHoaDon,LoaiHoaDon, ID_DonVi, ID_CheckIn
	ORDER BY NgayLapHoaDon desc
END

");

			Sql(@"ALTER PROCEDURE [dbo].[ListHangHoaTheKhoTheoLoHang]
    @ID_HangHoa [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier],
    @ID_LoHang [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;

	SELECT ID_HoaDon, 
		MaHoaDon, 
		NgayLapHoaDon,
		LoaiHoaDon, 
		ID_DonVi,
		ID_CheckIn,		
		sum(table1.SoLuong) as SoLuong,
		max(table1.GiaVon) as GiaVon,
		ROUND(max(table1.TonKho),3) as TonKho,
		ROUND(sum(sum(table1.SoLuong)) over ( order by NgayLapHoaDon ),3) as LuyKeTonKho,
		case table1.LoaiHoaDon
			when 10 then case when table1.ID_CheckIn = @IDChiNhanh then N'Nhận chuyển hàng' else N'Chuyển hàng' end
			when 1 then N'Bán hàng'
			when 2 then N'Bảo hành'
			when 4 then N'Nhập hàng'
			when 6 then N'Khách trả hàng'
			when 7 then N'Trả hàng nhập'
			when 8 then N'Xuất kho'
			when 9 then N'Kiểm hàng'
			when 13 then N'Nhập kho nội bộ'
			when 14 then N'Nhập hàng khách thừa'
			when 18 then N'Điều chỉnh giá vốn'
		end as LoaiChungTu
		FROM
	(
    	SELECT hd.ID as ID_HoaDon, 
		hd.MaHoaDon, 
		hd.LoaiHoaDon, 
		hd.YeuCau, 
		hd.ID_CheckIn, 
		hd.ID_DonVi, 
		bhct.ThanhTien * dvqd.TyLeChuyenDoi as ThanhTien,
		bhct.TienChietKhau * dvqd.TyLeChuyenDoi TienChietKhau, 
		CASE WHEN hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4' and hd.LoaiHoaDon = 10 THEN hd.NgaySua ELSE hd.NgayLapHoaDon END as NgayLapHoaDon,    		
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.TonLuyKe, bhct.TonLuyKe_NhanChuyenHang) as TonKho,
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.GiaVon / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi),bhct.GiaVon_NhanChuyenHang / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi)) as GiaVon,	
		(case hd.LoaiHoaDon
			when 9 then bhct.SoLuong ---- Số lượng lệch = SLThucTe - TonKhoDB        (-) Giảm  (+): Tăng
			when 10 then
				case when hd.ID_CheckIn= @IDChiNhanh and hd.YeuCau = '4' then bhct.TienChietKhau  ---- da nhanhang
				else iif(hd.YeuCau = '4',- bhct.TienChietKhau,- bhct.SoLuong) end 
			--- xuat
			when 1 then - bhct.SoLuong
			when 2 then - bhct.SoLuong ---- hoadon baohanh
			when 7 then - bhct.SoLuong
			when 8 then - bhct.SoLuong
			--- conlai: nhap
			else bhct.SoLuong end
		) * dvqd.TyLeChuyenDoi as SoLuong
    	FROM BH_HoaDon hd
    	LEFT JOIN BH_HoaDon_ChiTiet bhct on hd.ID = bhct.ID_HoaDon
    	LEFT JOIN DonViQuiDoi dvqd on bhct.ID_DonViQuiDoi = dvqd.ID
    	LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID		
    	WHERE bhct.ID_LoHang = @ID_LoHang 
		and hd.LoaiHoaDon not in (3,19,25,31, 29)  --- 29.khoitao phutungxe
		and hh.ID = @ID_HangHoa 
		and hd.ChoThanhToan = 0 
		and (bhct.ChatLieu is null or bhct.ChatLieu!='2')
		and ((hd.ID_DonVi = @IDChiNhanh 
		and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null)) or (hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4'))
	) as table1
    group by ID_HoaDon, MaHoaDon, NgayLapHoaDon,LoaiHoaDon, ID_DonVi, ID_CheckIn
	ORDER BY NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateChiTietKiemKe_WhenEditCTHD]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDMin [datetime]
AS
BEGIN
    SET NOCOUNT ON;
	

  
		 ------- get all donviquydoi lienquan ---
			declare @tblQuyDoi table (ID_DonViQuiDoi uniqueidentifier, ID_HangHoa uniqueidentifier, 
				ID_LoHang uniqueidentifier, 
				TyLeChuyenDoi float,
				LaHangHoa bit)
			insert into @tblQuyDoi
			select * from dbo.fnGetAllHangHoa_NeedUpdateTonKhoGiaVon(@IDHoaDonInput)

			------ get all ctKiemKe need update ---
			select 
				hd.ID as ID_HoaDon,
				ct.ID as ID_ChiTietHoaDon,
				hd.NgayLapHoaDon,
				ct.SoLuong,
				qd.ID_HangHoa,
				ct.ID_LoHang,
				ct.ID_DonViQuiDoi,
				0 as TonDauKy
			into #ctNeed
			from BH_HoaDon hd
			join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
			join  @tblQuyDoi qd on ct.ID_DonViQuiDoi= qd.ID_DonViQuiDoi			
				and (ct.ID_LoHang = qd.ID_LoHang or ct.ID_LoHang is null and qd.ID_LoHang is null)
    		WHERE hd.ChoThanhToan = 0 
			AND hd.LoaiHoaDon = 9 
    		and hd.ID_DonVi = @IDChiNhanhInput 
			and hd.NgayLapHoaDon >= @NgayLapHDMin



			----- get tonkho dauky (theo hanghoa) ----
				select 
					ID as ID_ChiTietHoaDon,
					ID_LoHang,
					ID_HangHoa,
					TonLuyKe,
					ROW_NUMBER() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon desc) as RN
				into #tblTonLuyKe
				from
				(
				select ct.ID_HoaDon,	
					ct.ID,	
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
							from #ctNeed ctNeed 
							where ctNeed.ID_HangHoa = qd.ID_HangHoa 
							and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
							------ !important: so sánh ngày lập --> để lấy TonLuyke ---
							AND ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    						or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
							)
				)tblTonLuyKe
			

					


			---------- update TonkhoDB, SoLuongLech, GiaTriLech to BH_HoaDon_ChiTiet----
			update ctkiemke
			set	ctkiemke.TienChietKhau = ctNeed.TonDauKy, 
    			ctkiemke.SoLuong = ctkiemke.ThanhTien - ctNeed.TonDauKy, ---- soluonglech
    			ctkiemke.ThanhToan = ctkiemke.GiaVon * (ctkiemke.ThanhTien - ctNeed.TonDauKy) --- gtrilech = soluonglech * giavon
			from BH_HoaDon_ChiTiet ctkiemke
			join (
					------- vì TonLuyKe đang tính theo dvqd ---> TonDauKy cũng lấy theo dvqd --
				select tkDK.ID_ChiTietHoaDon,
					tkDK.TonLuyKe / qd.TyLeChuyenDoi as TonDauKy
				from
				(
					select ctNeed.ID_ChiTietHoaDon, 
						ctNeed.ID_DonViQuiDoi,
						ctNeed.ID_LoHang,
						tkDK.TonLuyKe
					from #ctNeed ctNeed
					join #tblTonLuyKe tkDK on ctNeed.ID_HangHoa = tkDK.ID_HangHoa 
					and (ctNeed.ID_LoHang = tkDK.ID_LoHang or (ctNeed.ID_LoHang is null and tkDK.ID_LoHang is null)) 
					and tkDK.RN= 1 ----- TonLyKe gần nhất (ngaylap desc) --
				) tkDK
				join @tblQuyDoi qd on tkDK.ID_DonViQuiDoi = qd.ID_DonViQuiDoi
				and (qd.ID_LoHang = tkDK.ID_LoHang or (qd.ID_LoHang is null and tkDK.ID_LoHang is null)) 
			) ctNeed on ctkiemke.ID = ctNeed.ID_ChiTietHoaDon


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
				where exists (select ctNeed.ID_HoaDon from #ctNeed ctNeed where ctNeed.ID_ChiTietHoaDon = ct.ID)
				group by ct.ID_HoaDon
			)ctKK on hdKK.ID = ctKK.ID_HoaDon		
			
			drop table #ctNeed		
			drop table #tblTonLuyKe

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetDuLieuChamCong]
    @IDChiNhanhs [nvarchar](max),
    @IDPhongBans [nvarchar](max),
    @IDCaLamViecs [nvarchar](max),
    @TextSearch [nvarchar](max),
    @FromDate [nvarchar](10),
    @ToDate [nvarchar](10),
    @CurrentPage [int],
    @PageSize [int],
	@TrangThaiNV varchar(10)
AS
BEGIN
		SET NOCOUNT ON;

    	declare @dtNow datetime = getdate()
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
		INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
		DECLARE @count int =  (Select count(*) from @tblSearchString);
    
    	declare @tblDonVi table(ID uniqueidentifier)
    	insert into @tblDonVi
    	select name from dbo.splitstring(@IDChiNhanhs)
		
    	declare @tblTrangThaiNV table(TrangThaiNV int)
    	insert into @tblTrangThaiNV
    	select name from dbo.splitstring(@TrangThaiNV)
    
    	declare @tblPhong table(ID uniqueidentifier)
    	if @IDPhongBans=''	
    		insert into @tblPhong
    		select ID from NS_PhongBan
    	else
    		insert into @tblPhong
    		select name from dbo.splitstring(@IDPhongBans)
    
    	declare @tblCaLamViec table(ID uniqueidentifier)
    	if @IDCaLamViecs='%%' OR  @IDCaLamViecs=''
			begin
				set  @IDCaLamViecs =''
    			insert into @tblCaLamViec
    			select ca.ID from NS_CaLamViec ca
    			join NS_CaLamViec_DonVi dvca on ca.id= dvca.ID_CaLamViec
    			where exists (select ID from @tblDonVi dv where dv.ID= dvca.ID_DonVi)
			end
    	else
    		insert into @tblCaLamViec
    		select name from dbo.splitstring(@IDCaLamViecs);
    
    with data_cte
    	as (
    		select 
    				nv.MaNhanVien,
    				nv.TenNhanVien,
					iif(nv.DaNghiViec='1', 0,isnull(nv.TrangThai,1)) as TrangThaiNV, -- danghiviec ~ daxoa
    				ca.MaCa,
    				ca.TenCa,
    				format(ca.GioVao,'HH:mm') as GioVao,
    				format(ca.GioRa,'HH:mm') as GioRa,
    				tblView.TuNgay,
    				tblView.DenNgay,
    				tblView.ID_PhieuPhanCa,
    				tblView.ID_NhanVien,
    				tblView.ID_CaLamViec,					
					cast(tblView.TongCongNV as float) as TongCongNV,
					tblView.SoPhutDiMuon,
					tblView.SoPhutOT,					
    				tblView.Thang,
    				tblView.Nam,
    				tblView.Ngay1, Ngay2, Ngay3, ngay4, ngay5, Ngay6, Ngay7, Ngay8, Ngay9, 
    				Ngay10, Ngay11, Ngay12,ngay13, ngay14, Ngay15,ngay16, ngay17, Ngay18,Ngay19,
    				Ngay20, Ngay21, Ngay22,ngay23, ngay24, Ngay25,ngay26, ngay27, Ngay28,Ngay29,
    				Ngay30, Ngay31,

    				case when Format1 >= TuNgay and Format1 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format1)) end else '1' end as DisNgay1,
					case when Format2 >= TuNgay and Format2 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format2)) end else '1' end as DisNgay2,
					case when Format3 >= TuNgay and Format3 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format3)) end else '1' end as DisNgay3,
					case when Format4 >= TuNgay and Format4 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format4)) end else '1' end as DisNgay4,
					case when Format5 >= TuNgay and Format5 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format5)) end else '1' end as DisNgay5,
					case when Format6 >= TuNgay and Format6 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format6)) end else '1' end as DisNgay6,
					case when Format7 >= TuNgay and Format7 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format7)) end else '1' end as DisNgay7,
					case when Format8 >= TuNgay and Format8 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format8)) end else '1' end as DisNgay8,
					case when Format9 >= TuNgay and Format9 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format9)) end else '1' end as DisNgay9,

					case when Format10 >= TuNgay and Format10 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format10)) end else '1' end as DisNgay10,
					case when Format11 >= TuNgay and Format11 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format11)) end else '1' end as DisNgay11,
					case when Format12 >= TuNgay and Format12 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format12)) end else '1' end as DisNgay12,
					case when Format13 >= TuNgay and Format13 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format13)) end else '1' end as DisNgay13,
					case when Format14 >= TuNgay and Format14 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format14)) end else '1' end as DisNgay14,
					case when Format15 >= TuNgay and Format15 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format15)) end else '1' end as DisNgay15,
					case when Format16 >= TuNgay and Format16 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format16)) end else '1' end as DisNgay16,
					case when Format17 >= TuNgay and Format17 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format17)) end else '1' end as DisNgay17,
					case when Format18 >= TuNgay and Format18 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format18)) end else '1' end as DisNgay18,
					case when Format19 >= TuNgay and Format19 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format19)) end else '1' end as DisNgay19,

					case when Format20 >= TuNgay and Format20 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format20)) end else '1' end as DisNgay20,
					case when Format21 >= TuNgay and Format21 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format21)) end else '1' end as DisNgay21,
					case when Format22 >= TuNgay and Format22 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format22)) end else '1' end as DisNgay22,
					case when Format23 >= TuNgay and Format23 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format23)) end else '1' end as DisNgay23,
					case when Format24 >= TuNgay and Format24 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format24)) end else '1' end as DisNgay24,
					case when Format25 >= TuNgay and Format25 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format25)) end else '1' end as DisNgay25,
					case when Format26 >= TuNgay and Format26 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format26)) end else '1' end as DisNgay26,
					case when Format27 >= TuNgay and Format27 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format27)) end else '1' end as DisNgay27,
					case when Format28 >= TuNgay and Format28 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format28)) end else '1' end as DisNgay28,
					case when Format29 >= TuNgay and Format29 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format29)) end else '1' end as DisNgay29,

					case when Format30 >= TuNgay and Format30 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format30)) end else '1' end as DisNgay30,
					case when Format31 >= TuNgay and Format31 <= DenNgay then case when LoaiPhanCa= 3 then '0' else 
						(select dbo.GetDayOfWeek_byPhieuPhanCa(ID_PhieuPhanCa, ID_CaLamViec,Format31)) end else '1' end as DisNgay31
    			from
    				( 
    			select tblRow.*, phieu.LoaiPhanCa,
    				format(phieu.TuNgay,'yyyy-MM-dd') as TuNgay, 
    				format(ISNULL(phieu.DenNgay,dateadd(month,1,getdate())),'yyyy-MM-dd') as DenNgay,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,1) as Format1,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,2) as Format2,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,3) as Format3,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,4) as Format4,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,5) as Format5,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,6) as Format6,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,7) as Format7,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,8) as Format8,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,9) as Format9,
    
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,10) as Format10,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,11) as Format11,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,12) as Format12,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,13) as Format13,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,14) as Format14,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,15) as Format15,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,16) as Format16,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,17) as Format17,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,18) as Format18,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,19) as Format19,
    
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,20) as Format20,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,21) as Format21,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,22) as Format22,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,23) as Format23,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,24) as Format24,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,25) as Format25,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,26) as Format26,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,27) as Format27,
    				DATEFROMPARTS (tblRow.Nam,tblRow.Thang,28) as Format28,
    				--- avoid error Ngay 29-02, 30-02
    				DATEADD(MONTH, (tblRow.Nam - 1900) * 12 + tblRow.Thang - 1 , 28) as Format29, 
    				DATEADD(MONTH, (tblRow.Nam - 1900) * 12 + tblRow.Thang - 1 , 29) as Format30, 
    				DATEADD(MONTH, (tblRow.Nam - 1900) * 12 + tblRow.Thang - 1 , 30) as Format31
    			from
    			(
    
    				select tblunion.ID as ID_PhieuPhanCa, tblunion.ID_NhanVien, tblunion.Nam, tblunion.Thang, tblunion.ID_CaLamViec,
						max(tblunion.TongCongNV) as TongCongNV,
						max(tblunion.SoPhutDiMuon) as SoPhutDiMuon,
						max(tblunion.SoPhutOT) as SoPhutOT,

    					max(Ngay1) as Ngay1,max(Ngay2) as Ngay2,max(Ngay3) as Ngay3,max(Ngay4) as Ngay4,max(Ngay5) as Ngay5,
    					max(Ngay6) as Ngay6,max(Ngay7) as Ngay7,max(Ngay8) as Ngay8,max(Ngay9) as Ngay9, max(Ngay10) as Ngay10,
    					max(Ngay11) as Ngay11,max(Ngay12) as Ngay12,max(Ngay13) as Ngay13,max(Ngay14) as Ngay14,max(Ngay15) as Ngay15,
    					max(Ngay16) as Ngay16,max(Ngay17) as Ngay17,max(Ngay18) as Ngay18,max(Ngay19) as Ngay19, max(Ngay20) as Ngay20,
    					max(Ngay21) as Ngay21,max(Ngay22) as Ngay22,max(Ngay23) as Ngay23,max(Ngay24) as Ngay24,max(Ngay25) as Ngay25,
    					max(Ngay26) as Ngay26,max(Ngay27) as Ngay27,max(Ngay28) as Ngay28,max(Ngay29) as Ngay29, 
    					max(Ngay30) as Ngay30, max(Ngay31) as Ngay31
    
    				from
    				(
    					select phieu.ID, phieu.Nam, phieu.Thang, nvphieu.ID_NhanVien, ca.ID as ID_CaLamViec ,
    						null as Ngay1, null as Ngay2, null as Ngay3,  null as Ngay4, null as Ngay5, null as Ngay6,null as Ngay7, null as Ngay8, null as Ngay9 , 
    						null as Ngay10, null as Ngay11, null as Ngay12,null as Ngay13, null as Ngay14, null as Ngay15,null as Ngay16, null as Ngay17, null as Ngay18,null as Ngay19,
    						null as Ngay20, null as Ngay21, null as Ngay22,null as Ngay23, null as Ngay24, null as Ngay25,null as Ngay26, null as Ngay27, null as Ngay28,null as Ngay29,
    						null as Ngay30, null as Ngay31, 0 as TongCongNV, 0 as SoPhutDiMuon, 0 as SoPhutOT
    					from NS_PhieuPhanCa_NhanVien nvphieu 				
    					join (select ID, 
								case when DenNgay is null then case when @ToDate < @dtNow then datepart(year,@ToDate) else datepart(year, @dtNow) end
									else
										 datepart(year,DenNgay) ---- DenNgay is not null: luôn lấy Nam theo phiếu phân ca ----
										----case when datepart(year,DenNgay) != datepart(year, @dtNow) 
										----	then datepart(year, @dtNow) else iif(TuNgay < @FromDate, datepart(year, @FromDate), datepart(year,TuNgay)) end 
											end as Nam, 
    							case when DenNgay is null then datepart(month,@FromDate)  else 
    							case when TuNgay < @FromDate then datepart(month,@FromDate) else datepart(month,TuNgay) end end as Thang
    						from  NS_PhieuPhanCa
    						where TrangThai != 0  and ((DenNgay is null and TuNgay <= @ToDate ) 
    							OR ((DenNgay is not null 
    								and ((DenNgay <= @ToDate and DenNgay >=  @FromDate )
    									or (DenNgay >= @ToDate  and TuNgay <= @ToDate)))))
    						) phieu on nvphieu.ID_PhieuPhanCa = phieu.ID						
    					join NS_PhieuPhanCa_CaLamViec caphieu on nvphieu.ID_PhieuPhanCa = caphieu.ID_PhieuPhanCa
    					join NS_CaLamViec ca on ca.ID= caphieu.ID_CaLamViec
    
    					union all
    
						select pivOut.*, congOut.TongCongNV, 
							congOut.SoPhutDiMuon,
							congOut.SoPhutOT

						from
    						(select piv.ID_PhieuPhanCa, piv.Nam,  piv.Thang, piv.ID_NhanVien, piv.ID_CaLamViec, [1] as Ngay1, [2] as Ngay2,[3] as Ngay3, [4] as Ngay4, [5] as Ngay5, [6] as Ngay6,[7] as Ngay7, [8] as Ngay8, [9] as Ngay9,
    							[10] as Ngay10, [11] as Ngay11, [12] as Ngay12,[13] as Ngay13, [14] as Ngay14, [15] as Ngay15, [16] as Ngay16,[17] as Ngay17, [18] as Ngay18, [19] as Ngay19,
    							[20] as Ngay20, [21] as Ngay21, [22] as Ngay22,[23] as Ngay23, [24] as Ngay24, [25] as Ngay25, [26] as Ngay26,[27] as Ngay27, [28] as Ngay28, [29] as Ngay29,
    							[30] as Ngay30, [31] as Ngay31
    						from
    						(
    						select phieu.ID as ID_PhieuPhanCa, bs.ID_NhanVien, bs.ID_CaLamViec, bs.ID_DonVi, DATEPART(DAY, bs.NgayCham) as Ngay,DATEPART(MONTH, bs.NgayCham) as Thang,DATEPART(YEAR, bs.NgayCham) as Nam,
    						bs.KyHieuCong	
    						from NS_CongBoSung bs
    						join NS_PhieuPhanCa_NhanVien phieunv on bs.ID_NhanVien = phieunv.ID_NhanVien
    						join NS_PhieuPhanCa_CaLamViec phieuca on phieunv.ID_PhieuPhanCa = phieuca.ID_PhieuPhanCa and  bs.ID_CaLamViec = phieuca.ID_CaLamViec
    						join NS_PhieuPhanCa phieu on phieunv.ID_PhieuPhanCa = phieu.ID
    						where phieu.TrangThai != 0
    							and ((DenNgay is null and TuNgay <= @ToDate) 
    								OR ((DenNgay is not null 
    									and ((DenNgay <= @ToDate and DenNgay >= @FromDate )
    									or (DenNgay >= @ToDate and TuNgay <= @ToDate )))))
    						and bs.NgayCham >= @FromDate and bs.NgayCham <=@ToDate
							and bs.TrangThai !=0
							and exists (select ID from @tblDonVi dv where dv.ID= bs.ID_DonVi)
    						) a
    						PIVOT (
    						  max(KyHieuCong)
    						  FOR Ngay in ( [1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19], [20],[21],[22],[23],[24],[25],[26],[27],[28],[29],[30],[31]) 
    						) piv 
						) pivOut
						join (
							-- sumcong ofnv
							select  
								cong2.ID_NhanVien, cong2.ID_CaLamViec,
								cong2.CongNgayThuong + cong2.CongNgayNghiLe as TongCongNV,
								cong2.SoPhutDiMuon,
								SoGioOT as SoPhutOT								
								from
								(
												select cong.ID_NhanVien, cong.ID_CaLamViec, cong.ID_DonVi,
    												sum(cong.CongNgayThuong) as CongNgayThuong,
    												sum(CongNgayNghiLe) as CongNgayNghiLe,
    												sum(OTNgayThuong) as OTNgayThuong,
    												sum(OTNgayNghiLe) as OTNgayNghiLe,
    												sum(SoPhutDiMuon) as SoPhutDiMuon,
													sum(SoGioOT) as SoGioOT
    											from
    												(select bs.ID_CaLamViec,  bs.ID_NhanVien, bs.ID_DonVi,
    													bs.Cong, bs.CongQuyDoi, bs.SoGioOT, bs.GioOTQuyDoi, bs.SoPhutDiMuon,
    													IIF(bs.LoaiNgay=0, bs.Cong,0) as CongNgayThuong,
    													IIF(bs.LoaiNgay!=0, bs.Cong,0) as CongNgayNghiLe,
    													IIF(bs.LoaiNgay=0, bs.SoGioOT,0) as OTNgayThuong,
    													IIF(bs.LoaiNgay!=0, bs.SoGioOT,0) as OTNgayNghiLe
    												from NS_CongBoSung bs
    												join NS_CaLamViec ca on bs.ID_CaLamViec = ca.ID
    												where NgayCham >= @FromDate and NgayCham <= @ToDate
    												and bs.TrangThai !=0    		
													and  exists (select ID from @tblDonVi dv where dv.ID= bs.ID_DonVi) 
    												) cong group by cong.ID_NhanVien, cong.ID_CaLamViec, cong.ID_DonVi
											 ) cong2
						) congOut on pivout.ID_NhanVien= congOut.ID_NhanVien and pivout.ID_CaLamViec= congOut.ID_CaLamViec
    				) tblunion
    				group by  tblunion.ID,tblunion.ID_NhanVien, tblunion.Nam, tblunion.Thang, tblunion.ID_CaLamViec
    			) tblRow
    			join NS_PhieuPhanCa phieu on tblRow.ID_PhieuPhanCa = phieu.ID
    		) tblView 
    	join NS_CaLamViec ca on tblView.ID_CaLamViec = ca.ID
    	join NS_NhanVien nv on tblView.ID_NhanVien= nv.ID     	
    	where exists (select ID from @tblCaLamViec ca2 where ca.ID= ca2.ID) --- van hien nv daxoa de check bang cong cu
		and exists (select TrangThaiNV from @tblTrangThaiNV tt where iif(nv.DaNghiViec='1', 0,isnull(nv.TrangThai,1)) = tt.TrangThaiNV)
		and (@IDPhongBans ='' or 
				----- NV từng chấm công ở CN1, nay chuyển công tác sang CN2
					exists (select pb.ID from @tblPhong pb 
					join NS_QuaTrinhCongTac ct on pb.ID =  ct.ID_PhongBan
					where exists(select Name from dbo.splitstring(@IDChiNhanhs) dv where ct.ID_DonVi= dv.Name)))

    	AND ((select count(Name) from @tblSearchString b where    			
    				nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'
    				)=@count or @count=0)	
    	),
    	count_cte
    	as (
    	SELECT COUNT(*) AS TotalRow, 
    			CEILING(COUNT(*) / CAST(@PageSize as float )) as TotalPage ,
				SUM(TongCongNV) as TongCongAll,
				SUM(SoPhutDiMuon) as TongSoPhutDiMuon
    	from data_cte
    	)
    	select dt.*, cte.*
    	from data_cte dt
    	cross join count_cte cte		
    	order by dt.TuNgay
    	OFFSET (@CurrentPage* @PageSize) ROWS
    	FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[ChangePTN_updateCus]
    @ID_PhieuTiepNhan [uniqueidentifier],
    @ID_KhachHangOld [uniqueidentifier],
    @ID_BaoHiemOld [uniqueidentifier],
    @Types [nvarchar](20)
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @tblType table(Loai int)
    	insert into @tblType select name from dbo.splitstring(@Types)
    
    	---- get PTN new
    	declare @PTNNew_IDCusNew uniqueidentifier, @PTNNew_BaoHiem uniqueidentifier, @PTNNew_IdXe uniqueidentifier
    	select @PTNNew_IdXe = ID_Xe, @PTNNew_IDCusNew = ID_KhachHang, @PTNNew_BaoHiem = ID_BaoHiem from Gara_PhieuTiepNhan where ID= @ID_PhieuTiepNhan
    
    	---- get list hoadon of PTN
    	select ID, ID_DoiTuong, ID_BaoHiem
    	into #tblHoaDon
    	from BH_HoaDon
    	where ID_PhieuTiepNhan = @ID_PhieuTiepNhan
    	and ChoThanhToan is not null
    	and LoaiHoaDon in (3,25)

		----  alway update IdXe new
		update hd set  ID_Xe = @PTNNew_IdXe
    	from BH_HoaDon hd
    	join #tblHoaDon hdCheck on hd.ID= hdCheck.ID
    
    	---- update cus,
    	if (select count(*) from @tblType where Loai in ('1','3')) > 0
    	begin
    		update hd set ID_DoiTuong= @PTNNew_IDCusNew
    		from BH_HoaDon hd
    		join #tblHoaDon hdCheck on hd.ID= hdCheck.ID

			---- update phieuthu khachhang
				update qct set ID_DoiTuong= @PTNNew_IDCusNew
    			from Quy_HoaDon_ChiTiet qct
    			join #tblHoaDon hdCheck on qct.ID_HoaDonLienQuan= hdCheck.ID
    			where qct.ID_DoiTuong = hdCheck.ID_DoiTuong
    	end

    
    	---- update baohiem
    	if (select count(*) from @tblType where Loai in ('2','4')) > 0
    	begin
    		update hd set ID_BaoHiem= @PTNNew_BaoHiem
    		from BH_HoaDon hd
    		join #tblHoaDon hdCheck on hd.ID= hdCheck.ID
    				
				---- update phieuthu baohiem
    		update qct set ID_DoiTuong= @PTNNew_BaoHiem
    		from Quy_HoaDon_ChiTiet qct
    		join #tblHoaDon hdCheck on qct.ID_HoaDonLienQuan= hdCheck.ID
    		where qct.ID_DoiTuong = hdCheck.ID_BaoHiem
    	end

	
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetInforProduct_ByIDQuiDoi]
    @IDQuiDoi [uniqueidentifier],
    @ID_ChiNhanh [uniqueidentifier],
	@ID_LoHang uniqueidentifier = null
AS
BEGIN
    SET NOCOUNT ON;
	if	@ID_LoHang is null
		set @ID_LoHang='00000000-0000-0000-0000-000000000000'

		select  tbl.*, 
			isnull(tblVTtri.TenViTris,'') as ViTriKho
		from
		(
    		Select top 50
    			qd.ID as ID_DonViQuiDoi,
    			hh.ID,
    			qd.MaHangHoa,
    			hh.TenHangHoa,
    			qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    			qd.TenDonViTinh,
    			hh.LaHangHoa,
				hh.LoaiHangHoa,
    			Case When gv.ID is null then 0 else  gv.GiaVon end as GiaVon,
    			qd.GiaBan,
    			qd.GiaNhap,
				qd.Xoa,
				hh.TheoDoi,
				hh.DuocBanTrucTiep,
				isnull(tk.TonKho,0) as TonKho,			
    			Case when lh.ID is null then null else lh.ID end as ID_LoHang,
    			lh.MaLoHang,
    			lh.NgaySanXuat,
    			lh.NgayHetHan,
				qd.LaDonViChuan,
				hh.ID_NhomHang as ID_NhomHangHoa,
				Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end as PhiDichVu,
				Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
			case when ISNULL(QuyCach,0) = 0 then TyLeChuyenDoi else QuyCach * TyLeChuyenDoi end as QuyCach,
			ISNULL(hh.DonViTinhQuyCach,'0') as DonViTinhQuyCach,
			ISNULL(QuanLyTheoLoHang,'0') as QuanLyTheoLoHang,
			ISNULL(ThoiGianBaoHanh,0) as ThoiGianBaoHanh,
			ISNULL(LoaiBaoHanh,0) as LoaiBaoHanh,
			ISNULL(SoPhutThucHien,0) as SoPhutThucHien, 
			ISNULL(hh.GhiChu,'') as GhiChuHH ,
			ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio, 
			ISNULL(hh.DuocTichDiem,0) as DuocTichDiem
    	from DonViQuiDoi qd    	
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
    	left join DM_LoHang lh on qd.ID_HangHoa = lh.ID_HangHoa and (lh.TrangThai = 1 or lh.TrangThai is null)
		left join DM_HangHoa_TonKho tk on qd.ID = tk.ID_DonViQuyDoi and (lh.ID = tk.ID_LoHang or lh.ID is null) and tk.ID_DonVi = @ID_ChiNhanh
    	left join DM_GiaVon gv on qd.ID = gv.ID_DonViQuiDoi and (lh.ID = gv.ID_LoHang or lh.ID is null) and gv.ID_DonVi = @ID_ChiNhanh
    	where qd.ID = @IDQuiDoi
		and iif(@ID_LoHang='00000000-0000-0000-0000-000000000000', @ID_LoHang , lh.ID ) = @ID_LoHang
	) tbl
	left join
	(
	----- vitrikho -----
		select hh.ID,
				(
    			Select  vt.TenViTri + ',' AS [text()]
    			From dbo.DM_HangHoa_ViTri vt
				join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    			where hh.ID= vth.ID_HangHoa
    			For XML PATH ('')
			 ) TenViTris
		From DM_HangHoa hh
	) tblVTtri on tbl.ID = tblVTtri.ID

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListPhieuTiepNhan_v2] 
 --declare 
 @IdChiNhanhs [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
	@IdUserLogin uniqueidentifier = null,
    @NgayTiepNhan_From [datetime] =null,
    @NgayTiepNhan_To [datetime]= '2025-01-01',
    @NgayXuatXuongDuKien_From [datetime]=null,
    @NgayXuatXuongDuKien_To [datetime] ='2024-01-01',
    @NgayXuatXuong_From [datetime]=null,
    @NgayXuatXuong_To [datetime] ='2025-01-01',
    @TrangThais [nvarchar](20)='1,2,0,3',
    @TextSearch [nvarchar](max)='',
    @CurrentPage [int]=0,
    @PageSize [int]= 500,
	@BaoHiem int = 3
AS
BEGIN
    SET NOCOUNT ON;

	declare @TotalRow int = 0;
    
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
    
    	declare @tbTrangThai table (GiaTri varchar(2))
    	insert into @tbTrangThai
    	select Name from dbo.splitstring(@TrangThais);
    	if(@PageSize != 0)
    	BEGIN
    	
		;with data_cte
		as
		(
    	select ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, ptn.SoKmVao, ptn.NgayXuatXuongDuKien, ptn.NgayXuatXuong, ptn.TrangThai,
    	ptn.SoKmRa, ptn.TenLienHe, ptn.SoDienThoaiLienHe, ptn.GhiChu, ptn.TrangThai AS TrangThaiPhieuTiepNhan,
    	ptn.ID_Xe, dmx.BienSo, dmx.SoMay, dmx.SoKhung, dmx.NamSanXuat, mauxe.TenMauXe, hangxe.TenHangXe, loaixe.TenLoaiXe,
    	ptn.ID_KhachHang, dt.MaDoiTuong, dt.TenDoiTuong, dt.Email, dt.DienThoai AS DienThoaiKhachHang, dt.DiaChi,
    	ptn.ID_CoVanDichVu, ISNULL(nvcovan.TenNhanVien, '') AS CoVanDichVu, ISNULL(nvcovan.MaNhanVien, '') AS MaCoVan, nvcovan.DienThoaiDiDong AS CoVan_SDT,
    	ptn.ID_NhanVien, nvtiepnhan.MaNhanVien AS MaNhanVienTiepNhan, nvtiepnhan.TenNhanVien AS NhanVienTiepNhan,
    	dmx.DungTich, dmx.MauSon, dmx.HopSo,
    	cast(iif(dmx.ID_KhachHang = ptn.ID_KhachHang,'1','0') as bit) as LaChuXe,
		cx.ID as ID_ChuXe,
    	cx.TenDoiTuong as ChuXe,
    	cx.DienThoai as ChuXe_SDT, cx.DiaChi as ChuXe_DiaChi, cx.Email as ChuXe_Email,
		bh.TenDoiTuong as TenBaoHiem, bh.MaDoiTuong as MaBaoHiem, ptn.NguoiLienHeBH, ptn.SoDienThoaiLienHeBH,
    	dv.MaDonVi, dv.TenDonVi,
    	ptn.NgayTao, ptn.ID_BaoHiem, ptn.ID_DonVi,  ptn.NguoiTao
    	from Gara_PhieuTiepNhan ptn
    	inner join Gara_DanhMucXe dmx on ptn.ID_Xe = dmx.ID
    	LEFT join DM_DoiTuong cx on dmx.ID_KhachHang = cx.ID
    	inner join DM_DoiTuong dt on dt.ID = ptn.ID_KhachHang
		left join DM_DoiTuong bh on ptn.ID_BaoHiem = bh.ID
    	left join NS_NhanVien nvcovan on nvcovan.ID = ptn.ID_CoVanDichVu
    	inner join NS_NhanVien nvtiepnhan on nvtiepnhan.ID = ptn.ID_NhanVien
    	inner join Gara_MauXe mauxe on mauxe.ID = dmx.ID_MauXe
    	inner join Gara_HangXe hangxe on hangxe.ID = mauxe.ID_HangXe
    	inner join Gara_LoaiXe loaixe on loaixe.ID = mauxe.ID_LoaiXe
    	inner join DM_DonVi dv on dv.ID = ptn.ID_DonVi
    	inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    	WHERE exists (select GiaTri from @tbTrangThai tt where ptn.TrangThai = tt.GiaTri)
    		AND (@NgayTiepNhan_From IS NULL OR ptn.NgayVaoXuong BETWEEN @NgayTiepNhan_From AND @NgayTiepNhan_To)
    		AND (@NgayXuatXuongDuKien_From IS NULL OR ptn.NgayXuatXuongDuKien BETWEEN @NgayXuatXuongDuKien_From AND @NgayXuatXuongDuKien_To)
    		AND (@NgayXuatXuong_From IS NULL OR ptn.NgayXuatXuong BETWEEN @NgayXuatXuong_From AND @NgayXuatXuong_To)
			AND ((@BaoHiem = 0 AND 1 = 0) OR (@BaoHiem = 1 AND ptn.ID_BaoHiem IS NOT NULL) OR (@BaoHiem = 2 AND ptn.ID_BaoHiem IS NULL)
			OR @BaoHiem = 3 AND 1 = 1)
    		AND ((select count(Name) from @tblSearch b where     			
    		ptn.MaPhieuTiepNhan like '%'+b.Name+'%'
    		or ptn.GhiChu like '%'+b.Name+'%'
    		or dt.MaDoiTuong like '%'+b.Name+'%'		
    		or dt.TenDoiTuong like '%'+b.Name+'%'
    		or dt.DienThoai like '%'+b.Name+'%'
    		or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    		or nvcovan.TenNhanVien like '%'+b.Name+'%'	
    		or nvcovan.MaNhanVien like '%'+b.Name+'%'	
    		or nvcovan.TenNhanVienKhongDau like '%'+b.Name+'%'	
    		or nvcovan.TenNhanVienChuCaiDau like '%'+b.Name+'%'	
    		or nvtiepnhan.TenNhanVien like '%'+b.Name+'%'	
    		or nvtiepnhan.MaNhanVien like '%'+b.Name+'%'	
    		or nvtiepnhan.TenNhanVienChuCaiDau like '%'+b.Name+'%'	
    		or nvtiepnhan.TenNhanVienKhongDau like '%'+b.Name+'%'	
    		or ptn.TenLienHe like '%'+b.Name+'%'	
    		or ptn.SoDienThoaiLienHe like '%'+b.Name+'%'
    		or dmx.BienSo like '%'+b.Name+'%'
			or mauxe.TenMauXe like '%'+b.Name+'%'
			or hangxe.TenHangXe like '%'+b.Name+'%'
			or loaixe.TenLoaiXe like '%'+b.Name+'%'
			or bh.TenDoiTuong like '%'+b.Name+'%'
			or bh.MaDoiTuong like '%'+b.Name+'%'
    		)=@count or @count=0)
			),
    		count_cte
    		as
    		(
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    			from data_cte
    		)
			select *
			from data_cte dt
			cross join count_cte
			ORDER BY dt.NgayVaoXuong desc
			OFFSET (@CurrentPage * @PageSize) ROWS
			FETCH NEXT @PageSize ROWS ONLY

    		
    	END
    	ELSE
    	BEGIN
    		
    		
    		select ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, ptn.SoKmVao, ptn.NgayXuatXuongDuKien, ptn.NgayXuatXuong, ptn.TrangThai,
    		ptn.SoKmRa, ptn.TenLienHe, ptn.SoDienThoaiLienHe, ptn.GhiChu, ptn.TrangThai AS TrangThaiPhieuTiepNhan,
    		ptn.ID_Xe, dmx.BienSo, dmx.SoMay, dmx.SoKhung, dmx.NamSanXuat, mauxe.TenMauXe, hangxe.TenHangXe, loaixe.TenLoaiXe,
    		ptn.ID_KhachHang, dt.MaDoiTuong, dt.TenDoiTuong, dt.Email, dt.DienThoai AS DienThoaiKhachHang, dt.DiaChi,
    		ptn.ID_CoVanDichVu, ISNULL(nvcovan.TenNhanVien, '') AS CoVanDichVu, ISNULL(nvcovan.MaNhanVien, '') AS MaCoVan, nvcovan.DienThoaiDiDong AS CoVan_SDT,
    		ptn.ID_NhanVien, nvtiepnhan.MaNhanVien AS MaNhanVienTiepNhan, nvtiepnhan.TenNhanVien AS NhanVienTiepNhan,
    		dmx.DungTich, dmx.MauSon, dmx.HopSo,
			cast(iif(dmx.ID_KhachHang = ptn.ID_KhachHang,'1','0') as bit) as LaChuXe,
			cx.ID as ID_ChuXe,
    		cx.TenDoiTuong as ChuXe,
    		cx.DienThoai as ChuXe_SDT, cx.DiaChi as ChuXe_DiaChi, cx.Email as ChuXe_Email,
    		dv.MaDonVi, dv.TenDonVi,
			bh.TenDoiTuong as TenBaoHiem, bh.MaDoiTuong as MaBaoHiem, ptn.NguoiLienHeBH, ptn.SoDienThoaiLienHeBH,
    		ptn.NgayTao, ptn.ID_BaoHiem,ptn.ID_DonVi, ptn.NguoiTao
    		from Gara_PhieuTiepNhan ptn
    		inner join Gara_DanhMucXe dmx on ptn.ID_Xe = dmx.ID
    		LEFT join DM_DoiTuong cx on dmx.ID_KhachHang = cx.ID
    		inner join DM_DoiTuong dt on dt.ID = ptn.ID_KhachHang
			left join DM_DoiTuong bh on ptn.ID_BaoHiem = bh.ID
    		left join NS_NhanVien nvcovan on nvcovan.ID = ptn.ID_CoVanDichVu
    		inner join NS_NhanVien nvtiepnhan on nvtiepnhan.ID = ptn.ID_NhanVien
    		inner join Gara_MauXe mauxe on mauxe.ID = dmx.ID_MauXe
    		inner join Gara_HangXe hangxe on hangxe.ID = mauxe.ID_HangXe
    		inner join Gara_LoaiXe loaixe on loaixe.ID = mauxe.ID_LoaiXe
    		inner join DM_DonVi dv on dv.ID = ptn.ID_DonVi
    		inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    		WHERE exists (select GiaTri from @tbTrangThai tt where ptn.TrangThai = tt.GiaTri)
    			AND (@NgayTiepNhan_From IS NULL OR ptn.NgayVaoXuong BETWEEN @NgayTiepNhan_From AND @NgayTiepNhan_To)
    			AND (@NgayXuatXuongDuKien_From IS NULL OR ptn.NgayXuatXuongDuKien BETWEEN @NgayXuatXuongDuKien_From AND @NgayXuatXuongDuKien_To)
    			AND (@NgayXuatXuong_From IS NULL OR ptn.NgayXuatXuong BETWEEN @NgayXuatXuong_From AND @NgayXuatXuong_To)
				AND ((@BaoHiem = 0 AND 1 = 0) OR (@BaoHiem = 1 AND ptn.ID_BaoHiem IS NOT NULL) OR (@BaoHiem = 2 AND ptn.ID_BaoHiem IS NULL)
				OR @BaoHiem = 3 AND 1 = 1)
    			AND ((select count(Name) from @tblSearch b where     			
    			ptn.MaPhieuTiepNhan like '%'+b.Name+'%'
    			or ptn.GhiChu like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'		
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.DienThoai like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or nvcovan.TenNhanVien like '%'+b.Name+'%'	
    			or nvcovan.MaNhanVien like '%'+b.Name+'%'	
    			or nvcovan.TenNhanVienKhongDau like '%'+b.Name+'%'	
    			or nvcovan.TenNhanVienChuCaiDau like '%'+b.Name+'%'	
    			or nvtiepnhan.TenNhanVien like '%'+b.Name+'%'	
    			or nvtiepnhan.MaNhanVien like '%'+b.Name+'%'	
    			or nvtiepnhan.TenNhanVienChuCaiDau like '%'+b.Name+'%'	
    			or nvtiepnhan.TenNhanVienKhongDau like '%'+b.Name+'%'	
    			or ptn.TenLienHe like '%'+b.Name+'%'	
    			or ptn.SoDienThoaiLienHe like '%'+b.Name+'%'
    			or dmx.BienSo like '%'+b.Name+'%'
				or mauxe.TenMauXe like '%'+b.Name+'%'
				or hangxe.TenHangXe like '%'+b.Name+'%'
				or loaixe.TenLoaiXe like '%'+b.Name+'%'
				or bh.TenDoiTuong like '%'+b.Name+'%'
				or bh.MaDoiTuong like '%'+b.Name+'%'
    			)=@count or @count=0)
    			
			
    	END
END");

			Sql(@"ALTER PROCEDURE [dbo].[Insert_ThongBaoHetTonKho]
  @ID_ChiNhanh uniqueidentifier,
   @LoaiHoaDon int,
  @tblHoaDonChiTiet ChiTietHoaDonEdit readonly
AS
BEGIN
    SET NOCOUNT ON;
	
	begin
	try

			select tk.ID_DonVi,
			qd.ID_HangHoa,
			tk.ID_DonViQuyDoi  as ID_DonViQuiDoi, tk.ID_LoHang, 
			tk.TonKho, 
			hh.TonToiDa / iif(qd.TyLeChuyenDoi= 0 or qd.TyLeChuyenDoi is null,1, qd.TyLeChuyenDoi) as TonToiDa,
			hh.TonToiThieu / iif(qd.TyLeChuyenDoi= 0 or qd.TyLeChuyenDoi is null,1, qd.TyLeChuyenDoi) as TonToiThieu,
			MaHangHoa,  lo.MaLoHang
		into #tblTKho
		from DM_HangHoa_TonKho tk
		join DonViQuiDoi qd on tk.ID_DonViQuyDoi = qd.ID
		join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
		left join DM_LoHang lo on lo.ID_HangHoa= hh.ID and ((lo.ID = tk.ID_LoHang) or (lo.ID is null and tk.ID_LoHang is null))
		where tk.ID_DonVi = @ID_ChiNhanh
		and hh.LaHangHoa = 1
		and exists( select ID_DonViQuyDoi from @tblHoaDonChiTiet qd2 where qd2.ID_DonViQuiDoi= qd.ID)
		and (exists( select ID_LoHang from @tblHoaDonChiTiet lo2 where lo2.ID_LoHang= lo.ID) Or hh.QuanLyTheoLoHang= 0)  

		if  @LoaiHoaDon in (1, 7,8,10)
		begin
		insert into HT_ThongBao
    	select newid(), @ID_ChiNhanh,0, 
    	CONCAT(N'<p onclick=""loaddadoc(''key',''')""> Hàng hóa <a onclick=""loadthongbao(''0'', ''', MaHangHoa,''', ''key'')"">',   


		'<span class=""blue"">', MaHangHoa, ' </span>', N'</a> đã hết số lượng tồn kho. Vui lòng nhập thêm để tiếp tục kinh doanh </p>'),
    		GETDATE(),''


		from #tblTKho    
		where TonKho <= 0



		insert into HT_ThongBao
		select newid(), @ID_ChiNhanh,0, 
    		CONCAT(N'<p onclick=""loaddadoc(''key', ''')""> Hàng hóa <a onclick=""loadthongbao(''0'', ''', MaHangHoa, ''', ''key'')"">',
		'<span class=""blue"">', MaHangHoa, ' </span>', N'</a> sắp hết hàng trong kho. Vui lòng nhập thêm để tiếp tục kinh doanh </p>'),
    		GETDATE(),''


		from #tblTKho  
		where TonKho < TonToiThieu and TonKho > 0


		end




		if  @LoaiHoaDon = 4


		begin
			insert into HT_ThongBao



				select newid(), @ID_ChiNhanh,0, 
    			CONCAT(N'<p onclick=""loaddadoc(''key', ''')""> Hàng hóa <a onclick=""loadthongbao(''0'', ''', MaHangHoa, ''', ''key'')"">',
			'<span class=""blue"">', MaHangHoa, ' </span>', N'</a> đã vượt quá số lượng tồn kho quy định </p>'),
    			GETDATE(),''


			from #tblTKho    
			where TonKho > TonToiDa and TonToiDa > 0


		end

		end try


		begin catch



		end catch

END");

			Sql(@"ALTER PROCEDURE [dbo].[CTHD_GetChiPhiDichVu]
    @IDHoaDons [nvarchar](max),
    @IDVendors [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	declare @sql nvarchar(max) ='', @where nvarchar(max), @tblDefined nvarchar(max) ='',
    	@paramDefined nvarchar(max) ='@IDHoaDons_In nvarchar(max), @IDVendors_In nvarchar(max)'
    
    	set @where=' where 1 = 1 and (cthd.ID_ParentCombo is null or cthd.ID_ParentCombo != cthd.ID)
    	   and (cthd.ID_ChiTietDinhLuong is null or cthd.ID_ChiTietDinhLuong = cthd.ID)	'
    
    	if isnull(@IDHoaDons,'')!=''
    		begin
    			set @tblDefined = concat(@tblDefined, ' declare @tblHoaDon table (ID uniqueidentifier)
    			insert into @tblHoaDon select name from dbo.splitstring(@IDHoaDons_In)')
    			set @where = concat(@where,' and exists (select hd2.ID from @tblHoaDon hd2 where hd.ID = hd2.ID)') 
    		end
    	if isnull(@IDVendors,'')!=''
    		begin
    			set @tblDefined = concat(@tblDefined, ' declare @tblVendor table (ID uniqueidentifier)
    			insert into @tblVendor select name from dbo.splitstring(@IDVendors_In)')
    			set @where =concat(@where, ' and exists (select ncc.ID from @tblVendor ncc where cp.ID_NhaCungCap = ncc.ID)' )
    		end
    
    	set @sql= CONCAT(N'
    		select 
    			iif(cp.ID is null, ''00000000-0000-0000-0000-000000000000'',cp.ID) as ID,	
    			qd.MaHangHoa,
    			qd.TenDonViTinh,
    			cthd.ID_DonViQuiDoi,	
    			cthd.DonGia as GiaBan,
    			cp.ID_NhaCungCap,
				cp.GhiChu,
    			iif(cp.ID_HoaDon_ChiTiet is null, cthd.ID,cp.ID_HoaDon_ChiTiet) as ID_HoaDon_ChiTiet,
    			iif(cp.ID_HoaDon is null, cthd.ID_HoaDon,cp.ID_HoaDon) as ID_HoaDon,
    			dt.DienThoai,
    			dt.MaDoiTuong as MaNhaCungCap,
    			dt.TenDoiTuong as TenNhaCungCap,
    			iif(cp.SoLuong is null, cthd.SoLuong,cp.SoLuong) as SoLuong,
    			iif(cp.DonGia is null, 0,cp.DonGia) as DonGia,			
    			iif(cp.ThanhTien is null, 0,cp.ThanhTien) as ThanhTien,
    			xe.BienSo,
    			hd.ChiPhi as TongChiPhi,
    			hd.NgayLapHoaDon,
    			hd.MaHoaDon,
    			cthd.Soluong as SoLuongHoaDon, --- soluong max
    			iif(cthd.TenHangHoaThayThe is null or cthd.TenHangHoaThayThe ='''', hh.TenHangHoa, cthd.TenHangHoaThayThe) as TenHangHoaThayThe,
    			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa =''1'',1,2), hh.LoaiHangHoa) as LoaiHangHoa
    		from BH_HoaDon_ChiTiet cthd
    		join BH_HoaDon hd on cthd.ID_HoaDon= hd.ID
    		left join BH_HoaDon_ChiPhi cp on cthd.ID= cp.ID_HoaDon_ChiTiet and (cthd.ChatLieu is null or cthd.ChatLieu !=''5'')
    		left join DonViQuiDoi qd on cthd.ID_DonViQuiDoi= qd.ID
    	   left join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    	   left join DM_DoiTuong dt on cp.ID_NhaCungCap= dt.ID
    	   left join Gara_DanhMucXe xe on hd.ID_Xe= xe.ID
    	   ', @where,
    	 ' order by qd.MaHangHoa ')
    
    	 set @sql = concat(@tblDefined, @sql)
    
    	 exec sp_executesql @sql, @paramDefined,
    		@IDHoaDons_In = @IDHoaDons,
    		@IDVendors_In = @IDVendors
END");

			Sql(@"ALTER PROCEDURE [dbo].[LoadDanhMuc_KhachHangNhaCungCap]
    @IDChiNhanhs [nvarchar](max) = 'd93b17ea-89b9-4ecf-b242-d03b8cde71de',
    @LoaiDoiTuong [int] = 1,
    @IDNhomKhachs [nvarchar](max) = '',
    @TongBan_FromDate [datetime],
    @TongBan_ToDate [datetime],
    @NgayTao_FromDate [datetime],
    @NgayTao_ToDate [datetime],
    @TextSearch [nvarchar](max) = ' ',
    @Where [nvarchar](max) = ' ',
    @ColumnSort [nvarchar](40) = 'NgayTao',
    @SortBy [nvarchar](40) = 'DESC',
    @CurrentPage [int] = 0,
    @PageSize [int] = 20
AS
BEGIN
    SET NOCOUNT ON;
    	declare @whereCus nvarchar(max), @whereInvoice nvarchar(max), @whereLast nvarchar(max), 
    	@whereNhomKhach nvarchar(max),	@whereChiNhanh nvarchar(max),
    	@sql nvarchar(max) , @sql1 nvarchar(max), @sql2 nvarchar(max), @sql3 nvarchar(max),@sql4 nvarchar(max),
    	@paramDefined nvarchar(max)
    
    		declare @tblDefined nvarchar(max) = concat(N' declare @tblChiNhanh table (ID uniqueidentifier) ',	
    												   N' declare @tblIDNhoms table (ID uniqueidentifier) ',
    												   N' declare @tblSearch table (Name nvarchar(max))'    											 
    												   )
    
    
    		set @whereInvoice =' where 1 = 1 and hd.ChoThanhToan = 0 '
    		set @whereCus =' where 1 = 1 and dt.LoaiDoiTuong = @LoaiDoiTuong_In '		
    		set @whereLast = N' where tbl.ID not like ''00000000-0000-0000-0000-000000000%'' '
    		set @whereNhomKhach =' ' 
    		set @whereChiNhanh =' where 1 = 1 ' 
    
    		if isnull(@CurrentPage,'')=''
    			set @CurrentPage =0
    		if isnull(@PageSize,'')=''
    			set @PageSize = 10

		
    
    		if isnull(@ColumnSort,'')=''
    			set @ColumnSort = 'NgayTao'
    		if isnull(@SortBy,'')=''
    			set @SortBy = 'DESC'
    
    		set @sql1= 'declare @count int = 0, @TextSearch_Unsign nvarchar(max) ='''''
				if ISNULL(@TextSearch,'')!=''
					set @sql1 = CONCAT(@sql1, ' set @TextSearch_Unsign = (select dbo.FUNC_ConvertStringToUnsign(@TextSearch_In))' )
    
    		declare @QLTheoCN bit = '0'
    		if ISNULL(@IDChiNhanhs,'')!=''
    			begin								
    				set @QLTheoCN = (select max(cast(QuanLyKhachHangTheoDonVi as int)) from HT_CauHinhPhanMem 
    					where exists (select * from dbo.splitstring(@IDChiNhanhs) cn where ID_DonVi= cn.Name))
    
    				set @sql1 = concat(@sql1,
    				N' insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In)')
    
    				set @whereChiNhanh= CONCAT(@whereChiNhanh, ' and exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID)')
    				set @whereInvoice= CONCAT(@whereInvoice, ' and exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID)')
    			end
    		
    
    		if ISNULL(@IDNhomKhachs,'')='' ---- idNhom = empty
    			begin			
    				set @sql1 = concat(@sql1,
    				N' insert into @tblIDNhoms(ID) values (''00000000-0000-0000-0000-000000000000'')')
    
    				if @QLTheoCN = 1
    					begin
    						set @sql1 = concat(@sql1, N' insert into @tblIDNhoms(ID)
    						select * 
    						from (
    						-- get Nhom not not exist in NhomDoiTuong_DonVi
    						select ID from DM_NhomDoiTuong nhom  
    						where not exists (select ID_NhomDoiTuong from NhomDoiTuong_DonVi where ID_NhomDoiTuong= nhom.ID) 
    						and LoaiDoiTuong = @LoaiDoiTuong_In
    						union all
    						-- get Nhom at this ChiNhanh
    						select ID_NhomDoiTuong  from NhomDoiTuong_DonVi ', @whereChiNhanh,
    						N' ) tbl ')	
    						
    						set @whereNhomKhach  = CONCAT(@whereNhomKhach,
    						N' and EXISTS(SELECT Name FROM splitstring(tbl.ID_NhomDoiTuong) lstFromtbl 
    								inner JOIN @tblIDNhoms tblsearch ON lstFromtbl.Name = tblsearch.ID where lstFromtbl.Name!='''' )')	
    					end										
    			end
    		else
    		begin
    			set @sql1=  CONCAT(@sql1, N' insert into @tblIDNhoms values ( CAST(@IDNhomKhachs_In as uniqueidentifier) ) ')
    			set @whereNhomKhach  = CONCAT(@whereNhomKhach,
    			N' and EXISTS(SELECT Name FROM splitstring(tbl.ID_NhomDoiTuong) lstFromtbl 
    					inner JOIN @tblIDNhoms tblsearch ON lstFromtbl.Name = tblsearch.ID where lstFromtbl.Name!='''' )')			
    		end
    
    		if isnull(@TextSearch,'') !=''
    			begin
    			--	set @sql1= CONCAT(@sql1, N' 
    			--	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!='''';
    			--Select @count =  (Select count(*) from @tblSearch);')
    
    			--	set @whereLast = CONCAT(@whereLast,
    			--	 N' and ((select count(Name) from @tblSearch b where 				
    			--	 tbl.Name_Phone like ''%''+b.Name+''%''    		
    			--	)=@count or @count=0)')
				set @whereLast = CONCAT(@whereLast, N' and (MaDoiTuong like N''%''+ @TextSearch_In +''%'' 
							or TenDoiTuong  like N''%''+ @TextSearch_In +''%'' 
							or TenDoiTuong_KhongDau  like N''%''+ @TextSearch_In +''%''
							or DienThoai like N''%''+ @TextSearch_In +''%''
							or Email like N''%''+ @TextSearch_In +''%''
							or TenDoiTuong  like N''%''+ @TextSearch_Unsign +''%'' 
							or TenDoiTuong_KhongDau  like N''%''+ @TextSearch_Unsign +''%''
							)')
    			end
    
    		if isnull(@NgayTao_FromDate,'') !=''
    			if isnull(@NgayTao_ToDate,'') !=''
    				begin
    					set @whereCus = CONCAT(@whereCus, N' and dt.NgayTao between @NgayTao_FromDate_In and @NgayTao_ToDate_In')
    				end
    
    		if isnull(@TongBan_FromDate,'') !=''
    			if isnull(@TongBan_ToDate,'') !=''
    				begin
    					set @whereInvoice = CONCAT(@whereInvoice, N' and hd.NgayLapHoaDon between @TongBan_FromDate_In and @TongBan_ToDate_In')
    				end

			print @whereInvoice
    
    		if ISNULL(@Where,'')!=''
    			begin
    				set @Where = CONCAT(@whereLast, @whereNhomKhach, ' and ', @Where)
    			end
    		else
    			begin
    				set @Where = concat(@whereLast, @whereNhomKhach)
    			end
    		
    	set @sql2 = concat(
    		N'
    	;with data_cte
    	as
    	(
    		select *
    		from
    		(
    		select 
    			dt.*,
    			round(isnull(a.NoHienTai,0),2) as NoHienTai,
    			isnull(a.TongBan,0) as TongBan,
    			isnull(a.TongMua,0) as TongMua,
    			isnull(a.TongBanTruTraHang,0) as TongBanTruTraHang,
    			cast(isnull(a.SoLanMuaHang,0) as float) as SoLanMuaHang,
    			isnull(a.PhiDichVu,0) as PhiDichVu,
    			CONCAT(dt.MaDoiTuong,'' '', dt.TenDoiTuong, '' '', dt.DienThoai, '' '', dt.TenDoiTuong_KhongDau) as Name_Phone
    		from (
    			select 
    				dt.ID,
    				dt.MaDoiTuong,
    				dt.TenDoiTuong,
    				dt.TenDoiTuong_KhongDau,
    				dt.TenDoiTuong_ChuCaiDau,
    				dt.LoaiDoiTuong,
    				dt.ID_TrangThai,
    				dt.ID_NguonKhach,
    				dt.ID_NhanVienPhuTrach,
    				dt.ID_NguoiGioiThieu,
    				dt.ID_DonVi,
    				dt.ID_TinhThanh,
    				dt.ID_QuanHuyen,
    				isnull(dt.TheoDoi,''0'') as TheoDoi,
    				dt.LaCaNhan,				
    				dt.GioiTinhNam,
    				dt.NgaySinh_NgayTLap,
    				dt.DinhDang_NgaySinh,
    				dt.NgayGiaoDichGanNhat,
    				dt.TaiKhoanNganHang,
    				isnull(dt.TenNhomDoiTuongs,N''Nhóm mặc định'') as TenNhomDT,
    				dt.NgayTao,
    				isnull(dt.TrangThai_TheGiaTri,1) as TrangThai_TheGiaTri,
    				isnull(dt.TongTichDiem,0) as TongTichDiem,
    				----isnull(dt.TheoDoi,''0'') as TrangThaiXoa,
    				isnull(dt.DienThoai,'''') as DienThoai,
    				isnull(dt.Email,'''') as Email,
    				isnull(dt.DiaChi,'''') as DiaChi,
    				isnull(dt.MaSoThue,'''') as MaSoThue,
    				isnull(dt.GhiChu,'''') as GhiChu,
    				ISNULL(dt.NguoiTao,'''') as NguoiTao,
    				iif(dt.IDNhomDoiTuongs='''' or dt.IDNhomDoiTuongs is null,''00000000-0000-0000-0000-000000000000'', dt.IDNhomDoiTuongs) as ID_NhomDoiTuong
    			from DM_DoiTuong dt ', @whereCus, N' )  dt
    			left join
    			(
    			select 
    				 tblThuChi.ID_DoiTuong,
    				SUM(ISNULL(tblThuChi.DoanhThu,0)) + SUM(ISNULL(tblThuChi.TienChi,0)) + sum(ISNULL(tblThuChi.ThuTuThe,0))
    						- sum(isnull(tblThuChi.PhiDichVu,0)) 
    						- SUM(ISNULL(tblThuChi.TienThu,0)) - SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS NoHienTai,
    				SUM(ISNULL(tblThuChi.DoanhThu, 0)) as TongBan,
    					sum(ISNULL(tblThuChi.ThuTuThe,0)) as ThuTuThe,
    				SUM(ISNULL(tblThuChi.DoanhThu,0)) -  SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS TongBanTruTraHang,
    				SUM(ISNULL(tblThuChi.GiaTriTra, 0)) - SUM(ISNULL(tblThuChi.DoanhThu, 0)) as TongMua,
    				SUM(ISNULL(tblThuChi.SoLanMuaHang, 0)) as SoLanMuaHang,
    				sum(isnull(tblThuChi.PhiDichVu,0)) as PhiDichVu ')
    		set @sql3=concat( N' from
    			(
    				---- chiphi dv ncc ----
    				select 
    					cp.ID_NhaCungCap as ID_DoiTuong,
    					0 as GiaTriTra,
    					0 as DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi, 
    					0 AS SoLanMuaHang,
    					0 as ThuTuThe,
    					sum(iif(cp.ID_NhaCungCap = hd.ID_DoiTuong,0,cp.ThanhTien)) as PhiDichVu
    				from BH_HoaDon_ChiPhi cp
    				join BH_HoaDon hd on cp.ID_HoaDon= hd.ID
    				', @whereChiNhanh,
					 N' and hd.ChoThanhToan = 0 ',
    				 N' group by cp.ID_NhaCungCap
    
    				union all
    				----- tongban ----
    				SELECT 
    					hd.ID_DoiTuong,    	
    					0 as GiaTriTra,
    					hd.PhaiThanhToan as DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi, 
    					0 AS SoLanMuaHang,
    					0 as ThuTuThe,
    					0 as PhiDichVu
    			FROM BH_HoaDon hd ', @whereInvoice, N'  and hd.LoaiHoaDon in (1,7,19,25) ',
    
    				N' union all
    				---- doanhthu tuthe
    				SELECT 
    					hd.ID_DoiTuong,    	
    					0 as GiaTriTra,
    					0 as DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi, 
    					0 AS SoLanMuaHang,
    					hd.PhaiThanhToan as ThuTuThe,
    					0 as PhiDichVu
    			FROM BH_HoaDon hd ', @whereInvoice , N' and hd.LoaiHoaDon = 22 ', 
    
    					N' union all
    				-- gia tri trả từ bán hàng
    				SELECT 
    					hd.ID_DoiTuong,    	
						iif(cp.ID_NhaCungCap != hd.ID_DoiTuong, hd.PhaiThanhToan - isnull(hd.TongChiPhi,0), hd.PhaiThanhToan) as GiaTriTra,
    					0 as DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi, 
    					0 AS SoLanMuaHang,
    					0 as ThuTuThe,
    					0 as PhiDichVu
    			FROM BH_HoaDon hd
				left join BH_HoaDon_ChiPhi cp on hd.ID = cp.ID_HoaDon and cp.ID_HoaDon_ChiTiet is null
				',  @whereInvoice, N'  and hd.LoaiHoaDon in (6,4) ',
    				
    				N' union all
    				----- tienthu/chi ---
    				SELECT 
    					qct.ID_DoiTuong,						
    					0 AS GiaTriTra,
    					0 AS DoanhThu,
    					iif(qhd.LoaiHoaDon=11,qct.TienThu,0) AS TienThu,
    					iif(qhd.LoaiHoaDon=12,qct.TienThu,0) AS TienChi,
    					0 AS SoLanMuaHang,
    					0 as ThuTuThe,
    					0 as PhiDichVu
    			FROM Quy_HoaDon qhd
    			JOIN Quy_HoaDon_ChiTiet qct ON qhd.ID = qct.ID_HoaDon ',
    			@whereChiNhanh, 
    			N' and (qhd.TrangThai != 0 OR qhd.TrangThai is null)
    			and qct.HinhThucThanhToan!= 6
    			and (qct.LoaiThanhToan is null or qct.LoaiThanhToan != 3)
    				
    			
    
    				union all
    				----- solanmuahang ---
    				Select 
    					hd.ID_DoiTuong,
    					0 AS GiaTriTra,
    					0 AS DoanhThu,
    					0 AS TienThu,
    					0 as TienChi,
    					COUNT(*) AS SoLanMuaHang,
    					0 as ThuTuThe,
    					0 as PhiDichVu
    			From BH_HoaDon hd ' , @whereInvoice ,  N' and hd.LoaiHoaDon in (1,19,25) ',
    			N' group by hd.ID_DoiTuong
    			)tblThuChi 
    			GROUP BY tblThuChi.ID_DoiTuong
    		) a on dt.ID= a.ID_DoiTuong 
    		) tbl ', @Where ,
    	'), 
    	count_cte
    	as
    	(
    		SELECT COUNT(ID) AS TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize_In as float)) as TotalPage,
					SUM(TongBan) as TongBanAll,
    				SUM(TongBanTruTraHang) as TongBanTruTraHangAll,
    				SUM(TongTichDiem) as TongTichDiemAll,
    				SUM(NoHienTai) as NoHienTaiAll,
    				SUM(PhiDichVu) as TongPhiDichVu
    		from data_cte
    	),
    	tView
    	as (
    	select *		
    	from data_cte dt
    	cross join count_cte cte
    	ORDER BY ', @ColumnSort, ' ', @SortBy,
    	N' offset (@CurrentPage_In * @PageSize_In) ROWS
    	fetch next @PageSize_In ROWS only
    	)
    	select dt.*,
    		 ISNULL(trangthai.TenTrangThai,'''') as TrangThaiKhachHang,
    	ISNULL(qh.TenQuanHuyen,'''') as PhuongXa,
    	ISNULL(tt.TenTinhThanh,'''') as KhuVuc,
    	ISNULL(dv.TenDonVi,'''') as ConTy,
    	ISNULL(dv.SoDienThoai,'''') as DienThoaiChiNhanh,
    	ISNULL(dv.DiaChi,'''') as DiaChiChiNhanh,
    	ISNULL(nk.TenNguonKhach,'''') as TenNguonKhach,
    	ISNULL(dt2.TenDoiTuong,'''') as NguoiGioiThieu,
    		ISNULL(nvpt.MaNhanVien,'''') as MaNVPhuTrach,
    		ISNULL(nvpt.TenNhanVien,'''') as TenNhanVienPhuTrach
    	from tView dt
    	left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    	LEFT join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    	LEFT join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID
    	LEFT join DM_DoiTuong dt2 on dt.ID_NguoiGioiThieu = dt2.ID
    	LEFT join NS_NhanVien nvpt on dt.ID_NhanVienPhuTrach = nvpt.ID
    	LEFT join DM_DonVi dv on dt.ID_DonVi = dv.ID
    	LEFT join DM_DoiTuong_TrangThai trangthai on dt.ID_TrangThai = trangthai.ID
    	')
    
    		set @sql = CONCAT(@tblDefined, @sql1, @sql2, @sql3)
    
    		set @paramDefined = N'@IDChiNhanhs_In nvarchar(max),
    								@LoaiDoiTuong_In int ,
    								@IDNhomKhachs_In nvarchar(max),
    								@TongBan_FromDate_In datetime,
    								@TongBan_ToDate_In datetime,
    								@NgayTao_FromDate_In datetime,
    								@NgayTao_ToDate_In datetime,
    								@TextSearch_In nvarchar(max),
    								@Where_In nvarchar(max) ,							
    								@ColumnSort_In varchar(40),
    								@SortBy_In varchar(40),
    								@CurrentPage_In int,
    								@PageSize_In int'
    
    		--print @sql
    		--print @sql2
    		--print @sql3
    
    
    		exec sp_executesql @sql, @paramDefined, 
    					@IDChiNhanhs_In = @IDChiNhanhs,
    					@LoaiDoiTuong_In= @LoaiDoiTuong,
    					@IDNhomKhachs_In= @IDNhomKhachs,
    					@TongBan_FromDate_In= @TongBan_FromDate,
    					@TongBan_ToDate_In =@TongBan_ToDate,
    					@NgayTao_FromDate_In =@NgayTao_FromDate ,
    					@NgayTao_ToDate_In = @NgayTao_ToDate,
    					@TextSearch_In = @TextSearch,
    					@Where_In = @Where ,
    					@ColumnSort_In = @ColumnSort,
    					@SortBy_In = @SortBy,
    					@CurrentPage_In = @CurrentPage,
    					@PageSize_In = @PageSize
END
");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_SoDuChiTiet]
    @Text_Search [nvarchar](max),
    @MaHH [nvarchar](max),
    @MaKH [nvarchar](max),
    @MaKH_TV [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
	@ThoiHan [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NhomHang_SP [nvarchar](max),
	@ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	set nocount on;
	declare @tblChiNhanh table( ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@ID_ChiNhanh)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';

    Select @count =  (Select count(*) from @tblSearchString);
	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung)

	declare @dtNow datetime = dateadd(day,-1, getdate())

	---- get gdvmua
	declare @tblCTMua table(
		MaHoaDon nvarchar(max),
		NgayLapHoaDon datetime,
		NgayApDungGoiDV datetime,
		HanSuDungGoiDV datetime,
		ID_DonVi uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID uniqueidentifier,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier,
		ID_LoHang uniqueidentifier,
		SoLuong float,
		DonGia float,
		TienChietKhau float,
		ThanhTien float,
		TongTienHang float,
		PTGiamGiaHD float)
	insert into @tblCTMua
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,@timeStart,@timeEnd
	
	
	---- get by idnhom, thoihan --> check where
				select *,
				concat(b.TenHangHoa, b.ThuocTinh_GiaTri) as TenHangHoaFull,
				b.GiaTriMua - b.GiaTriTra - b.GiaTriSD as GiaTriConLai
				from
				(
			
					select 
						ctm.ID_HoaDon,
						ctm.MaHoaDon,
						ctm.NgayLapHoaDon,
						ctm.NgayApDungGoiDV,
						ctm.HanSuDungGoiDV,
						dt.MaDoiTuong as MaKhachHang,
						dt.TenDoiTuong as TenKhachHang,
						dt.DienThoai,
						Case when dt.GioiTinhNam = 1 then N'Nam' else N'Nữ' end as GioiTinh,
						gt.TenDoiTuong as NguoiGioiThieu,
						nk.TenNguonKhach,
						isnull(dt.TenNhomDoiTuongs, N'Nhóm mặc định') as NhomKhachHang ,
						iif( hh.ID_NhomHang is null, '00000000-0000-0000-0000-000000000000',hh.ID_NhomHang) as ID_NhomHang,
						iif(@dtNow < ctm.HanSuDungGoiDV,1,0) as ThoiHan,						
						ctm.SoLuong,
						ctm.DonGia,
						ctm.SoLuong* ctm.DonGia as ThanhTienChuaCK,
						ctm.SoLuong*ctm.TienChietKhau as TienChietKhau,
						--ctm.ThanhTien,
						iif(ctm.MaHoaDon like N'import%', ctm.SoLuong * ctm.DonGia, ctm.ThanhTien) as ThanhTien ,
						--ctm.ThanhTien  * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						iif(ctm.MaHoaDon like N'import%', ctm.SoLuong * ctm.DonGia, ctm.ThanhTien) * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
						isnull(tbl.SoLuongSuDung,0) * iif(ctm.MaHoaDon like N'import%', ctm.DonGia, (ctm.DonGia - ctm.TienChietKhau)) * ( 1 - ctm.PTGiamGiaHD)  as GiaTriSD,
						--isnull(tbl.SoLuongSuDung,0) * (ctm.DonGia - ctm.TienChietKhau) * ( 1 - ctm.PTGiamGiaHD)  as GiaTriSD,
						iif(@XemGiaVon='0',0, round(isnull(tbl.GiaVon,0),2)) as GiaVon,
						round(ctm.SoLuong- isnull(tbl.SoLuongTra,0) - isnull(tbl.SoLuongSuDung,0),2)  as SoLuongConLai,
						qd.MaHangHoa,
						qd.TenDonViTinh,
						hh.TenHangHoa,
						qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
						lo.MaLoHang,
						nv.NhanVienChietKhau,
						ctm.ThanhTien * ctm.PTGiamGiaHD as GiamGiaHD ---- tinh giamgia theo ung mathnag
					from @tblCTMua ctm
					inner join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
					inner join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
					left join DM_LoHang lo on ctm.ID_LoHang= lo.ID
					left join DM_DoiTuong dt on ctm.ID_DoiTuong = dt.ID
					left join DM_DoiTuong gt on dt.ID_NguoiGioiThieu = gt.ID
					left join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID
					left join (Select Main.ID_ChiTietHoaDon,
					Left(Main.hanghoa_thuoctinh,Len(Main.hanghoa_thuoctinh)-2) As NhanVienChietKhau
    					From
    					(
    						Select distinct hh_tt.ID_ChiTietHoaDon,
    						(
    							Select tt.TenNhanVien + ' - ' AS [text()]
    							From (select nvth.ID_ChiTietHoaDon, nv.TenNhanVien from BH_NhanVienThucHien nvth 
    							inner join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID) tt
    							Where tt.ID_ChiTietHoaDon = hh_tt.ID_ChiTietHoaDon
    							For XML PATH ('')
    						) hanghoa_thuoctinh
    					From BH_NhanVienThucHien hh_tt
    					) Main) as nv on ctm.ID = nv.ID_ChiTietHoaDon
					left join (
						select 
							tblSD.ID_ChiTietGoiDV,
							sum(tblSD.SoLuongTra) as SoLuongTra,
							sum(tblSD.GiaTriTra) as GiaTriTra,
							sum(tblSD.SoLuongSuDung) as SoLuongSuDung,
							sum(tblSD.GiaVon) as GiaVon
						from 
						(
							---- hdsudung
							Select 								
								ct.ID_ChiTietGoiDV,														
								0 as SoLuongTra,
								0 as GiaTriTra,
								ct.SoLuong as SoLuongSuDung,
								ct.SoLuong * ct.GiaVon as GiaVon
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon in (1,25)
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
							and hd.NgayLapHoaDon > @timeStart
							and ct.ID_ChiTietGoiDV is not null

							union all
							--- hdtra
							Select 							
								ct.ID_ChiTietGoiDV,															
								ct.SoLuong as SoLuongTra,
								iif(hd.TongTienHang=0,0, ct.ThanhTien * (1- hd.TongGiamGia/hd.TongTienHang))  as GiaTriTra,			
								0 as SoLuongSuDung,
								0 as GiaVon
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
							and hd.NgayLapHoaDon > @timeStart
							and ct.ID_ChiTietGoiDV is not null
							--and exists (select ID from @tblCTMua ctm where ct.ID_ChiTietGoiDV= ctm.ID)
							)tblSD group by tblSD.ID_ChiTietGoiDV

					) tbl on ctm.ID= tbl.ID_ChiTietGoiDV
				where hh.LaHangHoa like @LaHangHoa
    			and hh.TheoDoi like @TheoDoi
    			and qd.Xoa like @TrangThai
				AND ((select count(Name) from @tblSearchString b where 
					ctm.MaHoaDon like '%'+b.Name+'%'
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or qd.MaHangHoa like '%'+b.Name+'%'
    				or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
    				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
					or dt.DienThoai like '%'+b.Name+'%'
    				or dt.MaDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoituong like '%'+b.Name+'%'
					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
					or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
					)=@count or @count=0)
			) b where b.ThoiHan like @ThoiHan
				and (b.ID_NhomHang like @ID_NhomHang or b.ID_NhomHang in (select * from splitstring(@ID_NhomHang_SP)))
				order by NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_SoDuTongHop]
    @Text_Search [nvarchar](max),
    @MaHH [nvarchar](max),
    @MaKH [nvarchar](max),
    @MaKH_TV [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
	@ThoiHan [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NhomHang_SP [nvarchar](max),
	@ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	declare @tblChiNhanh table( ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@ID_ChiNhanh)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';

    Select @count =  (Select count(*) from @tblSearchString);
	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From  HT_NguoiDung nd	   
    where nd.ID = @ID_NguoiDung)

	declare @dtNow datetime = getdate()

	---- get list GDV mua ---
	declare @tblCTMua table(
		MaHoaDon nvarchar(max),
		NgayLapHoaDon datetime,
		NgayApDungGoiDV datetime,
		HanSuDungGoiDV datetime,
		ID_DonVi uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID uniqueidentifier,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier,
		ID_LoHang uniqueidentifier,
		SoLuong float,
		DonGia float,
		TienChietKhau float,
		ThanhTien float,
		TongTienHang float,
		PTGiamGiaHD float)
	insert into @tblCTMua
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,@timeStart,@timeEnd
		
		Select 
			a.MaHoaDon,
			a.NgayLapHoaDon,
			a.NgayApDungGoiDV,
			a.HanSuDungGoiDV,
			a.MaKhachHang,
			a.TenKhachHang,
			a.DienThoai,
			a.GioiTinh,
			a.NhomKhachHang,
			a.TenNguonKhach,
			a.NguoiGioiThieu,
			sum(a.SoLuong) as SoLuong,
			sum(a.ThanhTien) as ThanhTien,
			sum(a.SoLuongTra) as SoLuongTra,
			sum(a.GiaTriTra) as GiaTriTra,
			sum(a.SoLuongSuDung) as SoLuongSuDung,
			sum(a.GiaTriMua) as GiaTriMua,
			sum(a.GiamGiaHD) as GiamGiaHD,
			sum(a.GiaTriSD) as GiaTriSD,
			sum(a.GiaTriConLai) as GiaTriConLai,
			iif(@XemGiaVon='0',cast( 0 as float),round( sum(a.GiaVon),2)) as GiaVon,
			round(sum(a.SoLuong) -  sum(a.SoLuongTra) - sum(a.SoLuongSuDung),2) as SoLuongConLai,
			CAST(ROUND(Case when DATEADD(day,-1,GETDATE()) <= MAX(a.HanSuDungGoiDV)
				then DATEDIFF(day,DATEADD(day,-1,GETDATE()),MAX(a.HanSuDungGoiDV)) else 0 end, 0) as float) as SoNgayConHan, 
			CAST(ROUND(Case when DATEADD(day,-1,GETDATE()) > MAX(a.HanSuDungGoiDV) 
			then DATEDIFF(day,DATEADD(day,-1,GETDATE()) ,MAX(a.HanSuDungGoiDV)) * (-1) else 0 end, 0) as float) as SoNgayHetHan			
		From
		(
				---- get by idnhom, thoihan --> check where
				select *,
					GiaTriMua - GiaTriTra - GiaTriSD as GiaTriConLai
				from
				(
			
					select 
						ctm.ID_HoaDon,
						ctm.MaHoaDon,
						ctm.NgayLapHoaDon,
						ctm.NgayApDungGoiDV,
						ctm.HanSuDungGoiDV,
						dt.MaDoiTuong as MaKhachHang,
						dt.TenDoiTuong as TenKhachHang,
						dt.DienThoai,
						Case when dt.GioiTinhNam = 1 then N'Nam' else N'Nữ' end as GioiTinh,
						gt.TenDoiTuong as NguoiGioiThieu,
						nk.TenNguonKhach,
						isnull(dt.TenNhomDoiTuongs, N'Nhóm mặc định') as NhomKhachHang ,
						iif( hh.ID_NhomHang is null, '00000000-0000-0000-0000-000000000000',hh.ID_NhomHang) as ID_NhomHang,
						iif(@dtNow <=ctm.HanSuDungGoiDV,1,0) as ThoiHan,						
						ctm.SoLuong,
					--	ctm.ThanhTien,
						iif(ctm.MaHoaDon like N'import%', ctm.SoLuong * ctm.DonGia, ctm.ThanhTien) as ThanhTien ,
						ctm.PTGiamGiaHD * ctm.ThanhTien as GiamGiaHD,
						-- ctm.ThanhTien  * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						iif(ctm.MaHoaDon like N'import%', ctm.SoLuong * ctm.DonGia, ctm.ThanhTien)  * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
						isnull(tbl.SoLuongSuDung,0) * iif(ctm.MaHoaDon like N'import%', ctm.DonGia, (ctm.DonGia - ctm.TienChietKhau)) * ( 1 - ctm.PTGiamGiaHD) as GiaTriSD,
						--isnull(tbl.SoLuongSuDung,0) * (ctm.DonGia - ctm.TienChietKhau) * ( 1 - ctm.PTGiamGiaHD)  as GiaTriSD,
						isnull(tbl.GiaVon,0) as GiaVon						
					from @tblCTMua ctm
					inner join DonViQuiDoi dvqd on ctm.ID_DonViQuiDoi = dvqd.ID
					inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
					left join DM_DoiTuong dt on ctm.ID_DoiTuong = dt.ID
					left join DM_DoiTuong gt on dt.ID_NguoiGioiThieu = gt.ID
					left join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID
					left join (
						select 
							tblSD.ID_ChiTietGoiDV,
							sum(tblSD.SoLuongTra) as SoLuongTra,
							sum(tblSD.GiaTriTra) as GiaTriTra,
							sum(tblSD.SoLuongSuDung) as SoLuongSuDung,
							sum(tblSD.GiaVon) as GiaVon
						from 
						(
							---- hdsudung
							Select 								
								ct.ID_ChiTietGoiDV,														
								0 as SoLuongTra,
								0 as GiaTriTra,
								ct.SoLuong as SoLuongSuDung,
								ct.SoLuong * ct.GiaVon as GiaVon
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							join @tblCTMua ctm on ct.ID_ChiTietGoiDV= ctm.ID
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon in (1,25)
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)

							union all
							--- hdtra
							Select 							
								ct.ID_ChiTietGoiDV,															
								ct.SoLuong as SoLuongTra,
								iif(hd.TongTienHang=0,0, ct.ThanhTien * (1- hd.TongGiamGia/hd.TongTienHang))  as GiaTriTra,								
								0 as SoLuongSuDung,
								0 as GiaVon
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							join @tblCTMua ctm on ct.ID_ChiTietGoiDV= ctm.ID
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
							)tblSD group by tblSD.ID_ChiTietGoiDV

					) tbl on ctm.ID= tbl.ID_ChiTietGoiDV
				where hh.LaHangHoa like @LaHangHoa
    			and hh.TheoDoi like @TheoDoi
    			and dvqd.Xoa like @TrangThai
				AND ((select count(Name) from @tblSearchString b where 
					ctm.MaHoaDon like '%'+b.Name+'%'
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or dvqd.MaHangHoa like '%'+b.Name+'%'
    				or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
    				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
					or dt.DienThoai like '%'+b.Name+'%'
    				or dt.MaDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoituong like '%'+b.Name+'%'
					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
					or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
					)=@count or @count=0)
			) b where b.ThoiHan like @ThoiHan
				and (b.ID_NhomHang like @ID_NhomHang or b.ID_NhomHang in (select * from splitstring(@ID_NhomHang_SP)))
			) a
    	Group by a.MaHoaDon,
			a.NgayLapHoaDon,
			a.NgayApDungGoiDV,
			a.HanSuDungGoiDV,
			a.MaKhachHang,
			a.TenKhachHang,
			a.DienThoai,
			a.GioiTinh,
			a.NhomKhachHang,
			a.TenNguonKhach,
			a.NguoiGioiThieu
    	order by a.NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDoanhThuSuaChuaTheoCoVan]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @SoLanTiepNhanFrom [float],
    @SoLanTiepNhanTo [float],
    @SoLuongHoaDonFrom [float],
    @SoLuongHoaDonTo [float],
    @DoanhThuFrom [float],
    @DoanhThuTo [float],
    @LoiNhuanFrom [float],
    @LoiNhuanTo [float],
    @TextSearch [nvarchar](max),
	@TrangThai NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
	DECLARE @tblTrangThai TABLE (TrangThai INT);
		insert into @tblTrangThai
    		select Name from dbo.splitstring(@TrangThai);

    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearch);
    
    	DECLARE @tblHoaDonSuaChua TABLE (IDCoVan UNIQUEIDENTIFIER, MaNhanVien NVARCHAR(MAX), TenNhanVien NVARCHAR(MAX), 
    	IDPhieuTiepNhan UNIQUEIDENTIFIER, IDHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, GiamTruBH FLOAT, DoanhThu FLOAT, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT nv.ID, nv.MaNhanVien, nv.TenNhanVien, ptn.ID, hd.ID, hd.NgayLapHoaDon, hd.GiamTruThanhToanBaoHiem, hd.TongThanhToan - hd.TongTienThue, dv.MaDonVi, dv.TenDonVi
    	FROM Gara_PhieuTiepNhan ptn
    	INNER JOIN BH_HoaDon hd ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN NS_NhanVien nv ON nv.ID = ptn.ID_CoVanDichVu
    	INNER JOIN DM_DonVi dv ON dv.ID = ptn.ID_DonVi
    	INNER JOIN @tblDonVi dvf ON dv.ID = dvf.ID_DonVi
		INNER JOIN @tblTrangThai tt ON tt.TrangThai = ptn.TrangThai
    	WHERE hd.LoaiHoaDon = 25 AND hd.ChoThanhToan = 0
    	AND (@ThoiGianFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @ThoiGianFrom AND @ThoiGianTo)
    	AND ((select count(Name) from @tblSearch b where     			
    			nv.MaNhanVien like '%'+b.Name+'%'
    			or nv.TenNhanVien like '%'+b.Name+'%'
    			)=@count or @count=0);
    
    	DECLARE @tblTienVon TABLE(IDCoVan UNIQUEIDENTIFIER, TienVon FLOAT);
    
    	INSERT INTO @tblTienVon
    	SELECT hdsc.IDCoVan, SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS TienVon
    	FROM (
			SELECT hdsc.IDCoVan, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    		FROM @tblHoaDonSuaChua hdsc
    		LEFT JOIN BH_HoaDon xk ON hdsc.IDHoaDon = xk.ID_HoaDon AND xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0
    		LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    		--WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
			UNION ALL
			SELECT hdsc.IDCoVan, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    		FROM (SELECT IDCoVan, IDPhieuTiepNhan FROM @tblHoaDonSuaChua GROUP BY IDCoVan, IDPhieuTiepNhan ) hdsc
    		INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan
    		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    		WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND xk.ID_HoaDon IS NULL
		) hdsc
    	GROUP BY hdsc.IDCoVan
    
    	DECLARE @SSoLanTiepNhan FLOAT, @SSoLuongHoaDon FLOAT, @STongDoanhThu FLOAT, @STienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT, @STongGiamTruBH FLOAT;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(IDCoVan UNIQUEIDENTIFIER, MaNhanVien NVARCHAR(MAX), TenNhanVien NVARCHAR(MAX),
    	SoLanTiepNhan FLOAT, SoLuongHoaDon FLOAT, TongGiamTruBH FLOAT, TongDoanhThu FLOAT, TongTienVon FLOAT, LoiNhuan FLOAT, NgayGiaoDichGanNhat DATETIME, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT)
    	
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hd.IDCoVan, hd.MaNhanVien, hd.TenNhanVien, hd.SoLanTiepNhan, hd.SoLuongHoaDon, ISNULL(hd.TongGiamTruBH, 0) AS TongGiamTruBH,
    	ISNULL(hd.TongDoanhThu,0) AS TongDoanhThu, ISNULL(tv.TienVon,0) AS TongTienVon, ISNULL(hd.TongDoanhThu,0) - ISNULL(tv.TienVon,0) AS LoiNhuan, hd.NgayGiaoDichGanNhat, hd.MaDonVi, hd.TenDonVi, 0
    	FROM
    	(
    	SELECT IDCoVan, MaNhanVien, TenNhanVien, MaDonVi, TenDonVi, COUNT(DISTINCT IDPhieuTiepNhan) AS SoLanTiepNhan, COUNT(IDHoaDon) AS SoLuongHoaDon, SUM(GiamTruBH) AS TongGiamTruBH, SUM(DoanhThu) AS TongDoanhThu,
    	MAX(NgayLapHoaDon) AS NgayGiaoDichGanNhat
    	FROM @tblHoaDonSuaChua
    	GROUP BY IDCoVan, MaNhanVien, TenNhanVien, MaDonVi, TenDonVi) AS hd
    	INNER JOIN @tblTienVon tv ON hd.IDCoVan = tv.IDCoVan
    	WHERE (@SoLanTiepNhanFrom IS NULL OR hd.SoLanTiepNhan >= @SoLanTiepNhanFrom)
    	AND (@SoLanTiepNhanTo IS NULL OR hd.SoLanTiepNhan <= @SoLanTiepNhanTo)
    	AND (@SoLuongHoaDonFrom IS NULL OR hd.SoLuongHoaDon >= @SoLuongHoaDonFrom)
    	AND (@SoLuongHoaDonTo IS NULL OR hd.SoLuongHoaDon <= @SoLuongHoaDonTo)
    	AND (@DoanhThuFrom IS NULL OR hd.TongDoanhThu >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR hd.TongDoanhThu <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR hd.TongDoanhThu - tv.TienVon >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR hd.TongDoanhThu - tv.TienVon <= @LoiNhuanTo)
		
		DECLARE @tblChiPhi TABLE (IDCoVan UNIQUEIDENTIFIER, ChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdsc.IDCoVan, SUM(hdcp.ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblHoaDonSuaChua hdsc ON hdcp.ID_HoaDon = hdsc.IDHoaDon
		GROUP BY hdsc.IDCoVan;

		UPDATE bcdt
		SET bcdt.ChiPhi = hdcp.ChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.ChiPhi FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.IDCoVan = hdcp.IDCoVan;

    	SELECT @SSoLanTiepNhan = SUM(SoLanTiepNhan), @SSoLuongHoaDon = SUM(SoLuongHoaDon), @STongDoanhThu = SUM(TongDoanhThu), @STienVon = SUM(TongTienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi), @STongGiamTruBH = SUM(TongGiamTruBH) FROM @tblBaoCaoDoanhThu
    
    	SELECT *, CAST(@SSoLanTiepNhan AS FLOAT) AS SSoLanTiepNhan, @SSoLuongHoaDon AS SSoLuongHoaDon, @STongGiamTruBH AS STongGiamTruBH, @STongDoanhThu AS STongDoanhThu, @STienVon AS STienVon, @SLoiNhuan AS SLoiNhuan, @SChiPhi AS SChiPhi FROM @tblBaoCaoDoanhThu
    	ORDER BY TenNhanVien
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDoanhThuSuaChuaTheoXe]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @SoLanTiepNhanFrom [float],
    @SoLanTiepNhanTo [float],
    @SoLuongHoaDonFrom [float],
    @SoLuongHoaDonTo [float],
    @DoanhThuFrom [float],
    @DoanhThuTo [float],
    @LoiNhuanFrom [float],
    @LoiNhuanTo [float],
    @TextSearch [nvarchar](max),
	@TrangThai NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
 --   DECLARE @IdChiNhanhs [nvarchar](max),
 --   @ThoiGianFrom [datetime],
 --   @ThoiGianTo [datetime],
 --   @SoLanTiepNhanFrom [float],
 --   @SoLanTiepNhanTo [float],
 --   @SoLuongHoaDonFrom [float],
 --   @SoLuongHoaDonTo [float],
 --   @DoanhThuFrom [float],
 --   @DoanhThuTo [float],
 --   @LoiNhuanFrom [float],
 --   @LoiNhuanTo [float],
 --   @TextSearch [nvarchar](max);
	--SET @IdChiNhanhs = 'd93b17ea-89b9-4ecf-b242-d03b8cde71de';
	--SET @ThoiGianFrom = '2021-12-01';
	--SET @ThoiGianTo = '2022-01-01';
	--SET @TextSearch = '30A-794.76'
    -- Insert statements for procedure here
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
	DECLARE @tblTrangThai TABLE (TrangThai INT);
		insert into @tblTrangThai
    		select Name from dbo.splitstring(@TrangThai);

    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearch);
    
    	DECLARE @tblHoaDonSuaChua TABLE (IDXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), SoMay NVARCHAR(MAX), SoKhung NVARCHAR(MAX), MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), DienThoai NVARCHAR(MAX), 
    	IDPhieuTiepNhan UNIQUEIDENTIFIER, IDHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, GiamTruBH FLOAT, DoanhThu FLOAT, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT dmx.ID, dmx.BienSo, dmx.SoMay, dmx.SoKhung,
    	dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, ptn.ID, hd.ID, hd.NgayLapHoaDon, hd.GiamTruThanhToanBaoHiem, hd.TongThanhToan - hd.TongTienThue, dv.MaDonVi, dv.TenDonVi
    	FROM Gara_PhieuTiepNhan ptn
    	INNER JOIN BH_HoaDon hd ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN Gara_DanhMucXe dmx ON ptn.ID_Xe = dmx.ID
    	LEFT JOIN DM_DoiTuong dt ON dt.ID = dmx.ID_KhachHang
    	INNER JOIN DM_DonVi dv ON dv.ID = ptn.ID_DonVi
    	INNER JOIN @tblDonVi dvf ON dv.ID = dvf.ID_DonVi
		INNER JOIN @tblTrangThai tt ON tt.TrangThai = ptn.TrangThai
    	WHERE hd.LoaiHoaDon = 25 AND hd.ChoThanhToan = 0
    	AND (@ThoiGianFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @ThoiGianFrom AND @ThoiGianTo)
    	AND ((select count(Name) from @tblSearch b where     			
    			dmx.BienSo like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dmx.SoMay like '%'+b.Name+'%'
    			or dmx.SoKhung like '%'+b.Name+'%'
    			or dt.DienThoai like '%'+b.Name+'%'
    			or dv.TenDonVi like '%'+b.Name + '%'
    			)=@count or @count=0);

				--SELECT * FROM @tblHoaDonSuaChua
    
    	DECLARE @tblTienVon TABLE(IDXe UNIQUEIDENTIFIER, TienVon FLOAT);
    
    	INSERT INTO @tblTienVon
    	SELECT hdsc.IDXe, SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS TienVon
    	FROM (
		SELECT hdsc.IDXe, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.IDHoaDon = xk.ID_HoaDon AND xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	--WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
		UNION ALL
		SELECT hdsc.IDXe, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM (SELECT IDPhieuTiepNhan, IDXe FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, IDXe) hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan
    	INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND xk.ID_HoaDon IS NULL
		) hdsc
    	GROUP BY hdsc.IDXe;
    
    	DECLARE @SSoLanTiepNhan FLOAT, @SSoLuongHoaDon FLOAT, @STongDoanhThu FLOAT, @STienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT, @STongGiamTruBH FLOAT;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(IDXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), SoKhung NVARCHAR(MAX), SoMay NVARCHAR(MAX), MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX),
    	DienThoai NVARCHAR(MAX), SoLanTiepNhan FLOAT, SoLuongHoaDon FLOAT, TongGiamTruBH FLOAT, TongDoanhThu FLOAT, TongTienVon FLOAT, LoiNhuan FLOAT, NgayGiaoDichGanNhat DATETIME, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT)
    	
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hd.IDXe, hd.BienSo, hd.SoKhung, hd.SoMay, hd.MaDoiTuong, hd.TenDoiTuong, hd.DienThoai, hd.SoLanTiepNhan, hd.SoLuongHoaDon,
    	ISNULL(hd.TongGiamTruBH, 0) AS TongGiamTruBH, ISNULL(hd.TongDoanhThu,0) AS TongDoanhThu, ISNULL(tv.TienVon,0) AS TongTienVon, ISNULL(hd.TongDoanhThu,0) - ISNULL(tv.TienVon,0) AS LoiNhuan, hd.NgayGiaoDichGanNhat, hd.MaDonVi, hd.TenDonVi, 0
    	FROM
    	(
    	SELECT IDXe, BienSo, SoMay, SoKhung,  MaDoiTuong, TenDoiTuong, DienThoai, MaDonVi, TenDonVi, COUNT(DISTINCT IDPhieuTiepNhan) AS SoLanTiepNhan, COUNT(IDHoaDon) AS SoLuongHoaDon, SUM(GiamTruBH) AS TongGiamTruBH, SUM(DoanhThu) AS TongDoanhThu,
    	MAX(NgayLapHoaDon) AS NgayGiaoDichGanNhat
    	FROM @tblHoaDonSuaChua
    	GROUP BY IDXe, BienSo, SoMay, SoKhung,  MaDoiTuong, TenDoiTuong, DienThoai, MaDonVi, TenDonVi) AS hd
    	LEFT JOIN @tblTienVon tv ON hd.IDXe = tv.IDXe
    	WHERE (@SoLanTiepNhanFrom IS NULL OR hd.SoLanTiepNhan >= @SoLanTiepNhanFrom)
    	AND (@SoLanTiepNhanTo IS NULL OR hd.SoLanTiepNhan <= @SoLanTiepNhanTo)
    	AND (@SoLuongHoaDonFrom IS NULL OR hd.SoLuongHoaDon >= @SoLuongHoaDonFrom)
    	AND (@SoLuongHoaDonTo IS NULL OR hd.SoLuongHoaDon <= @SoLuongHoaDonTo)
    	AND (@DoanhThuFrom IS NULL OR hd.TongDoanhThu >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR hd.TongDoanhThu <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR hd.TongDoanhThu - tv.TienVon >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR hd.TongDoanhThu - tv.TienVon <= @LoiNhuanTo);

		DECLARE @tblChiPhi TABLE (IDXe UNIQUEIDENTIFIER, ChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdsc.IDXe, SUM(hdcp.ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblHoaDonSuaChua hdsc ON hdcp.ID_HoaDon = hdsc.IDHoaDon
		GROUP BY hdsc.IDXe;

		UPDATE bcdt
		SET bcdt.ChiPhi = hdcp.ChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.ChiPhi FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.IDXe = hdcp.IDXe;
    
    	SELECT @SSoLanTiepNhan = SUM(SoLanTiepNhan), @SSoLuongHoaDon = SUM(SoLuongHoaDon), @STongDoanhThu = SUM(TongDoanhThu), @STienVon = SUM(TongTienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi), @STongGiamTruBH = SUM(TongGiamTruBH) FROM @tblBaoCaoDoanhThu
    
    	SELECT *, CAST(@SSoLanTiepNhan AS FLOAT) AS SSoLanTiepNhan, @SSoLuongHoaDon AS SSoLuongHoaDon, @STongGiamTruBH AS STongGiamTruBH, @STongDoanhThu AS STongDoanhThu, @STienVon AS STienVon, @SLoiNhuan AS SLoiNhuan, @SChiPhi AS SChiPhi FROM @tblBaoCaoDoanhThu
    	ORDER BY BienSo
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDoanhThuSuaChuaChiTiet]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @DoanhThuFrom [float],
    @DoanhThuTo [float],
    @LoiNhuanFrom [float],
    @LoiNhuanTo [float],
    @TextSearch [nvarchar](max),
	@IdNhomHangHoa UNIQUEIDENTIFIER,
	@TrangThai NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
	DECLARE @tblTrangThai TABLE (TrangThai INT);
		insert into @tblTrangThai
    		select Name from dbo.splitstring(@TrangThai);

	DECLARE @tblIdNhomHangHoa TABLE(ID UNIQUEIDENTIFIER);
	IF(@IdNhomHangHoa IS NOT NULL)
	BEGIN
		WITH parents AS (
		  SELECT ID, TenNhomHangHoa, ID_Parent
		  FROM DM_NhomHangHoa
		  WHERE ID = @IdNhomHangHoa

		  UNION ALL

		  SELECT nhh.ID, nhh.TenNhomHangHoa, nhh.ID_Parent
		  FROM DM_NhomHangHoa nhh
		  INNER JOIN parents p ON nhh.ID_Parent = p.ID
		)
		INSERT INTO @tblIdNhomHangHoa SELECT ID FROM parents;
	END
	ELSE
	BEGIN
		INSERT INTO @tblIdNhomHangHoa SELECT ID FROM DM_NhomHangHoa;
	END

    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearch);
    
    	DECLARE @tblHoaDonSuaChua TABLE (IDPhieuTiepNhan UNIQUEIDENTIFIER, MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, ID_DonViQuiDoi UNIQUEIDENTIFIER, IDChiTiet UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER,
    	MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), TenDonViTinh NVARCHAR(MAX), SoLuong FLOAT, DonGia FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, TienThue FLOAT,
    	GiamGia FLOAT, GiamTruBH FLOAT, DoanhThu FLOAT,
    	GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT);
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, dmx.BienSo, dt.MaDoiTuong, dt.TenDoiTuong, nv.TenNhanVien, hd.ID,
    	hd.MaHoaDon, hd.NgayLapHoaDon, hdct.ID_DonViQuiDoi, hdct.ID, hdct.ID_ChiTietDinhLuong,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, hdct.SoLuong, 
		IIF(hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL, hdct.DonGia, 0), 
		hdct.TienChietKhau*hdct.SoLuong, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),hdct.ThanhTien, 0) AS ThanhTien, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongThueKhachHang = 0 AND hd.TongTienThueBaoHiem <> 0, (hdct.DonGia - hdct.TienChietKhau)*hdct.SoLuong * (hd.TongTienThue/hd.TongTienHang), hdct.TienThue*hdct.SoLuong), 0) AS TienThue,
    	IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongTienHang = 0, 0, 
		hdct.ThanhTien * hd.TongGiamGia/hd.TongTienHang),0) AS GiamGia, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongTienHang = 0, 0, 
		hdct.ThanhTien * hd.GiamTruThanhToanBaoHiem/hd.TongTienHang),0) AS GiamGia, 
		IIF((hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL) AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID),
		IIF(hd.TongTienHang =0,hdct.ThanhTien,(hdct.ThanhTien * (1 - (hd.TongGiamGia + hd.GiamTruThanhToanBaoHiem)/hd.TongTienHang))),0) AS DoanhThu,
    	hdct.GhiChu, dv.MaDonVi, dv.TenDonVi, 0 FROM Gara_PhieuTiepNhan ptn
    	INNER JOIN BH_HoaDon hd ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN BH_HoaDon_ChiTiet hdct ON hd.ID = hdct.ID_HoaDon
    	INNER JOIN DonViQuiDoi dvqd ON hdct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
    	INNER JOIN Gara_DanhMucXe dmx ON ptn.ID_Xe = dmx.ID
    	INNER JOIN DM_DoiTuong dt ON dt.ID = ptn.ID_KhachHang
    	LEFT JOIN NS_NhanVien nv ON ptn.ID_CoVanDichVu = nv.ID
    	INNER JOIN DM_DonVi dv ON dv.ID = ptn.ID_DonVi
    	INNER JOIN @tblDonVi dvf ON dv.ID = dvf.ID_DonVi
		INNER JOIN @tblTrangThai tt ON tt.TrangThai = ptn.TrangThai
    	WHERE hd.LoaiHoaDon = 25 AND hd.ChoThanhToan = 0  --AND ptn.TrangThai != 0 
    	AND (@ThoiGianFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @ThoiGianFrom AND @ThoiGianTo)
    	AND ((select count(Name) from @tblSearch b where     			
    			ptn.MaPhieuTiepNhan like '%'+b.Name+'%'
    			or dmx.BienSo like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or nv.TenNhanVien like '%'+b.Name+'%'
    			or hd.MaHoaDon like '%'+b.Name+'%'
    			or hd.DienGiai like '%'+b.Name+'%'
				or hh.TenHangHoa like '%'+b.Name+'%'
				or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
				or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
				or dvqd.MaHangHoa like '%'+b.Name+'%'
				or hdct.GhiChu like '%'+b.Name+'%'
    			)=@count or @count=0);
		--SELECT * FROM @tblHoaDonSuaChua

		--SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, hdsc.IDChiTiet, hdsc.ID_ChiTietDinhLuong,
  --  	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
  --  	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
  --  	hdsc.MaHangHoa, hdsc.TenHangHoa, hdsc.TenDonViTinh, hdsc.SoLuong, hdsc.DonGia, hdsc.TienChietKhau, hdsc.ThanhTien, hdsc.TienThue,
  --  	hdsc.GiamGia, hdsc.DoanhThu,
  --  	hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
  --  	FROM @tblHoaDonSuaChua hdsc
  --  	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon
  --  	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
  --  	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL 

		DECLARE @tblGiaVonThanhPhan TABLE(IDChiTietDinhLuong UNIQUEIDENTIFIER, TongTIenVon FLOAT)
		INSERT INTO @tblGiaVonThanhPhan
		SELECT hdsc.ID_ChiTietDinhLuong, SUM(ISNULL(xkct.GiaVon,0) * ISNULL(xkct.SoLuong,0)) AS TongTienVon
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND hdsc.ID_ChiTietDinhLuong IS NOT NULL
		GROUP BY hdsc.ID_ChiTietDinhLuong
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), IDChiTiet UNIQUEIDENTIFIER, 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, MaHangHoa NVARCHAR(MAX), TenHangHoa NVARCHAR(MAX), TenDonViTinh NVARCHAR(MAX), SoLuong FLOAT, DonGia FLOAT, TienChietKhau FLOAT, ThanhTien FLOAT, 
    	TienThue FLOAT,
    	GiamGia FLOAT, GiamTruBH FLOAT, DoanhThu FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), TienVon FLOAT, LoiNhuan FLOAT, ChiPhi FLOAT)
    
    	INSERT INTO @tblBaoCaoDoanhThu
		SELECT bcsc.MaPhieuTiepNhan, bcsc.NgayVaoXuong, bcsc.BienSo, bcsc.IDChiTiet,
    	bcsc.MaDoiTuong, bcsc.TenDoiTuong, bcsc.CoVanDichVu,
    	bcsc.ID, bcsc.MaHoaDon, bcsc.NgayLapHoaDon,
    	bcsc.MaHangHoa, bcsc.TenHangHoa, bcsc.TenDonViTinh, bcsc.SoLuong, bcsc.DonGia, bcsc.TienChietKhau, bcsc.ThanhTien, bcsc.TienThue,
    	bcsc.GiamGia, bcsc.GiamTruBH, bcsc.DoanhThu,
    	bcsc.GhiChu, bcsc.MaDonVi, bcsc.TenDonVi, SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS TienVon,
    	bcsc.DoanhThu - SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS LoiNhuan, 0
		FROM
    	(
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, hdsc.IDChiTiet, hdsc.ID_ChiTietDinhLuong,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	hdsc.MaHangHoa, hdsc.TenHangHoa, hdsc.TenDonViTinh, hdsc.SoLuong, hdsc.DonGia, hdsc.TienChietKhau, hdsc.ThanhTien, hdsc.TienThue,
    	hdsc.GiamGia, hdsc.GiamTruBH, hdsc.DoanhThu,
    	hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0 and xk.LoaiHoaDon = 8
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	--WHERE xk.LoaiHoaDon = 8 OR xk.ID IS NULL 
		--AND (hdsc.IDChiTiet = hdsc.ID_ChiTietDinhLuong OR hdsc.ID_ChiTietDinhLuong IS NULL)
		UNION ALL
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, NULL, null,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, 0 AS SoLuong, 0 AS DonGia, 0 AS TienChietKhau, 0 AS ThanhTien, 0 AS TienThue,
    	0 AS GiamGia, 0 AS GiamTruBH, 0 AS DoanhThu,
    	'' AS GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk FROM
		(SELECT IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, 
		MaDonVi, TenDonVi, MaHoaDon, ID, NgayLapHoaDon
    	FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, 
		TenDonVi, MaHoaDon, ID, NgayLapHoaDon)
		hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon and xk.ChoThanhToan = 0
		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		INNER JOIN DonViQuiDoi dvqd ON xkct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 AND xkct.ID_ChiTietGoiDV IS NULL
		UNION ALL
		--Xuât kho cho phiếu tiếp nhận
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, NULL, null,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	null, '', null,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, 0, 0, 0, 0, 0,
    	0, 0, 0,
    	'', hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk FROM
		(SELECT IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, TenDonVi
    	FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, TenDonVi)
		hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan and xk.ChoThanhToan = 0
		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		INNER JOIN DonViQuiDoi dvqd ON xkct.ID_DonViQuiDoi = dvqd.ID
    	INNER JOIN DM_HangHoa hh ON hh.ID = dvqd.ID_HangHoa
		INNER JOIN @tblIdNhomHangHoa nhh ON hh.ID_NhomHang = nhh.ID
		--AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 AND xk.ID_HoaDon IS NULL
		) bcsc WHERE bcsc.IDChiTiet = bcsc.ID_ChiTietDinhLuong OR bcsc.ID_ChiTietDinhLuong IS NULL
		--AND ((select count(Name) from @tblSearch b where 
		--	MaHangHoa like '%'+b.Name+'%'
  --  			)=@count or @count=0) 
    	GROUP BY bcsc.MaPhieuTiepNhan, bcsc.NgayVaoXuong, bcsc.BienSo, bcsc.IDChiTiet, bcsc.ID_ChiTietDinhLuong,
    	bcsc.MaDoiTuong, bcsc.TenDoiTuong, bcsc.CoVanDichVu,
    	bcsc.ID, bcsc.MaHoaDon, bcsc.NgayLapHoaDon,
    	bcsc.MaHangHoa, bcsc.TenHangHoa, bcsc.TenDonViTinh, bcsc.SoLuong, bcsc.DonGia, bcsc.TienChietKhau, bcsc.ThanhTien, bcsc.TienThue,
    	bcsc.GiamGia, bcsc.GiamTruBH, bcsc.DoanhThu,
    	bcsc.GhiChu, bcsc.MaDonVi, bcsc.TenDonVi;

		UPDATE bcdt SET
		bcdt.TienVon = gvtp.TongTIenVon, bcdt.LoiNhuan = bcdt.DoanhThu - gvtp.TongTIenVon
		FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblGiaVonThanhPhan gvtp ON bcdt.IDChiTiet = gvtp.IDChiTietDinhLuong;
    
		DECLARE @tblChiPhi TABLE(IDChiTiet UNIQUEIDENTIFIER, ChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdcp.ID_HoaDon_ChiTiet, SUM(hdcp.ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblBaoCaoDoanhThu bcdt ON hdcp.ID_HoaDon_ChiTiet = bcdt.IDChiTiet
		GROUP BY hdcp.ID_HoaDon_ChiTiet

		UPDATE bcdt SET
		bcdt.ChiPhi = hdcp.ChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.ChiPhi
		FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.IDChiTiet = hdcp.IDChiTiet;

		DECLARE @tblGiamTruBaoHiem TABLE(ID UNIQUEIDENTIFIER, GiamTruThanhToanBaoHiem FLOAT);
		INSERT INTO @tblGiamTruBaoHiem
		SELECT ida.ID, hd.GiamTruThanhToanBaoHiem FROM 
		(SELECT ID FROM @tblBaoCaoDoanhThu GROUP BY ID) ida
		INNER JOIN BH_HoaDon hd ON ida.ID = hd.ID

		--UPDATE bcdt SET
		--bcdt.DoanhThu = bcdt.DoanhThu - bh.GiamTruThanhToanBaoHiem
		--FROM @tblBaoCaoDoanhThu bcdt
		--INNER JOIN @tblGiamTruBaoHiem bh ON bh.ID = bcdt.ID

    	DECLARE @SThanhTien FLOAT,  @SChietKhau FLOAT, @SThue FLOAT, @SGiamGia FLOAT, @SGiamTruBH FLOAT, @SDoanhThu FLOAT, @STongTienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT, @GiamTruThanhToanBaoHiem FLOAT;
		SELECT @GiamTruThanhToanBaoHiem = SUM(GiamTruThanhToanBaoHiem) FROM @tblGiamTruBaoHiem;
    	SELECT @SThanhTien = SUM(ThanhTien), @SChietKhau = SUM(TienChietKhau), @SThue = SUM(TienThue), @SGiamGia = SUM(GiamGia), @SGiamTruBH = SUM(GiamTruBH), 
		@SDoanhThu = SUM(DoanhThu), @STongTienVon = SUM(TienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi)
    	FROM @tblBaoCaoDoanhThu;
    
    	SELECT IDChiTiet, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu , ID AS IDHoaDon, MaHoaDon,
    	NgayLapHoaDon, MaHangHoa, TenHangHoa, TenDonViTinh, ISNULL(SoLuong, 0) AS SoLuong, ISNULL(DonGia, 0) AS DonGia, ISNULL(TienChietKhau, 0) AS TienChietKhau, 
    	ISNULL(TienThue,0) AS TienThue, ISNULL(ThanhTien,0) AS ThanhTien, ISNULL(GiamGia, 0) AS GiamGia, ISNULL(GiamTruBH, 0) AS GiamTruBH, ISNULL(DoanhThu, 0) AS DoanhThu, ISNULL(TienVon,0) AS TienVon, ISNULL(LoiNhuan,0) AS LoiNhuan,
    	GhiChu, MaDonVi, TenDonVi, ChiPhi, ISNULL(@SThanhTien, 0) AS SThanhTien, ISNULL(@SChietKhau,0) AS SChietKhau,
    	ISNULL(@SThue,0) AS SThue, ISNULL(@SGiamGia,0) AS SGiamGia, ISNULL(@SGiamTruBH,0) AS SGiamTruBH, ISNULL(@SDoanhThu, 0) AS SDoanhThu, ISNULL(@STongTienVon,0) AS STongTienVon,
    	ISNULL(@SLoiNhuan,0) AS SLoiNhuan, ISNULL(@SChiPhi, 0) AS SChiPhi
    	FROM @tblBaoCaoDoanhThu
    	WHERE (@DoanhThuFrom IS NULL OR DoanhThu >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR DoanhThu <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR LoiNhuan >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR LoiNhuan <= @LoiNhuanTo)
    	ORDER BY NgayLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDoanhThuSuaChuaTongHop]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @DoanhThuFrom [float],
    @DoanhThuTo [float],
    @LoiNhuanFrom [float],
    @LoiNhuanTo [float],
    @TextSearch [nvarchar](max),
	@TrangThai NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
		
		DECLARE @tblTrangThai TABLE (TrangThai INT);
		insert into @tblTrangThai
    		select Name from dbo.splitstring(@TrangThai);

    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearch);
    
    	DECLARE @tblHoaDonSuaChua TABLE (IDPhieuTiepNhan UNIQUEIDENTIFIER, MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, TongTienHang FLOAT, TongChietKhau FLOAT, TongTienThue FLOAT, TongChiPhi FLOAT,
    	TongGiamGia FLOAT, TongGiamTruBH FLOAT, TongThanhToan FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, dmx.BienSo, dt.MaDoiTuong, dt.TenDoiTuong, nv.TenNhanVien, hd.ID,
    	hd.MaHoaDon, hd.NgayLapHoaDon, SUM(hdct.SoLuong* hdct.DonGia), SUM(ISNULL(hdct.TienChietKhau, 0)*hdct.SoLuong), hd.TongTienThue, hd.TongChiPhi,
    	hd.TongGiamGia, hd.GiamTruThanhToanBaoHiem, hd.TongThanhToan - hd.TongTienThue, hd.DienGiai, dv.MaDonVi, dv.TenDonVi FROM Gara_PhieuTiepNhan ptn
    	INNER JOIN BH_HoaDon hd ON hd.ID_PhieuTiepNhan = ptn.ID
    	INNER JOIN BH_HoaDon_ChiTiet hdct ON hd.ID = hdct.ID_HoaDon
    	INNER JOIN Gara_DanhMucXe dmx ON ptn.ID_Xe = dmx.ID
    	INNER JOIN DM_DoiTuong dt ON dt.ID = ptn.ID_KhachHang
    	LEFT JOIN NS_NhanVien nv ON ptn.ID_CoVanDichVu = nv.ID
    	INNER JOIN DM_DonVi dv ON dv.ID = ptn.ID_DonVi
    	INNER JOIN @tblDonVi dvf ON dv.ID = dvf.ID_DonVi
		INNER JOIN @tblTrangThai tt ON tt.TrangThai = ptn.TrangThai
    	WHERE hd.LoaiHoaDon = 25 AND hd.ChoThanhToan = 0 AND (hdct.ID_ParentCombo = hdct.ID OR hdct.ID_ParentCombo IS NULL)
    	AND (@ThoiGianFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @ThoiGianFrom AND @ThoiGianTo)
    	AND ((select count(Name) from @tblSearch b where     			
    			ptn.MaPhieuTiepNhan like '%'+b.Name+'%'
    			or dmx.BienSo like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or nv.TenNhanVien like '%'+b.Name+'%'
    			or hd.MaHoaDon like '%'+b.Name+'%'
    			or hd.DienGiai like '%'+b.Name+'%'
    			)=@count or @count=0)
    	GROUP BY ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, dmx.BienSo, dt.MaDoiTuong, dt.TenDoiTuong, nv.TenNhanVien, hd.ID,
    	hd.MaHoaDon, hd.NgayLapHoaDon, hd.TongTienThue, hd.TongChiPhi,
    	hd.TongGiamGia, hd.TongThanhToan, hd.DienGiai, dv.MaDonVi, dv.TenDonVi, hd.GiamTruThanhToanBaoHiem;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, TongTienHang FLOAT, TongChietKhau FLOAT, TongTienThue FLOAT, TongChiPhi FLOAT,
    	TongGiamGia FLOAT, TongGiamTruBH FLOAT, TongThanhToan FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), GiaVon FLOAT, TienVon FLOAT, LoiNhuan FLOAT)
    
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, 
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon, hdsc.TongTienHang, hdsc.TongChietKhau, hdsc.TongTienThue, hdsc.TongChiPhi,
    	hdsc.TongGiamGia, hdsc.TongGiamTruBH, hdsc.TongThanhToan, hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, SUM(ISNULL(hdsc.GiaVon,0)) AS GiaVon, SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS TienVon,
    	hdsc.TongThanhToan - SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS LoiNhuan
    	FROM (
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, 
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon, hdsc.TongTienHang, hdsc.TongChietKhau, hdsc.TongTienThue, hdsc.TongChiPhi,
    	hdsc.TongGiamGia, hdsc.TongGiamTruBH, hdsc.TongThanhToan, hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0 AND xk.LoaiHoaDon = 8
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	--WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
		UNION ALL
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	NULL, '', null, 0, 0, 0, 0, 0, 0, 0, 
    	'', hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk FROM
		(SELECT IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, 
		MaDonVi, TenDonVi
    	FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu, MaDonVi, 
		TenDonVi)
		hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan
		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND xk.ID_HoaDon IS NULL
		) hdsc
    	GROUP BY hdsc.BienSo, hdsc.CoVanDichVu, hdsc.GhiChu, hdsc.ID, hdsc.MaDoiTuong, hdsc.MaHoaDon, 
    	hdsc.MaPhieuTiepNhan, hdsc.NgayLapHoaDon, hdsc.NgayVaoXuong, hdsc.TenDoiTuong,
    	hdsc.TongChietKhau, hdsc.TongChiPhi, hdsc.TongGiamGia, hdsc.TongGiamTruBH, hdsc.TongThanhToan,
    	hdsc.TongTienHang, hdsc.TongTienThue, hdsc.MaDonVi, hdsc.TenDonVi
		
		DECLARE @tblChiPhi TABLE(IDHoaDon UNIQUEIDENTIFIER, TongChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdcp.ID_HoaDon, SUM(ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblBaoCaoDoanhThu bcdt ON hdcp.ID_HoaDon = bcdt.ID
		GROUP BY hdcp.ID_HoaDon;

		UPDATE bcdt
		SET bcdt.TongChiPhi = hdcp.TongChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.TongChiPhi FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.ID = hdcp.IDHoaDon;

    	DECLARE @STongTienHang FLOAT,  @SChietKhau FLOAT, @SThue FLOAT, @SChiPhi FLOAT, @SGiamGia FLOAT, @SDoanhThu FLOAT, @STongTienVon FLOAT, @SLoiNhuan FLOAT, @SGiamTruBH FLOAT;
    	SELECT @STongTienHang = SUM(TongTienHang), @SChietKhau = SUM(TongChietKhau), @SThue = SUM(TongTienThue),
    	@SChiPhi = SUM(TongChiPhi), @SGiamGia = SUM(TongGiamGia), @SDoanhThu = SUM(TongThanhToan), @STongTienVon = SUM(TienVon), @SLoiNhuan = SUM(LoiNhuan),
		@SGiamTruBH = SUM(TongGiamTruBH)
    	FROM @tblBaoCaoDoanhThu
    
    	SELECT MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu , ID AS IDHoaDon, MaHoaDon,
    	NgayLapHoaDon, ISNULL(TongTienHang, 0) AS TongTienHang, ISNULL(TongChietKhau, 0) AS TongChietKhau, ISNULL(TongTienThue, 0) AS TongTienThue, 
    	ISNULL(TongChiPhi, 0) AS TongChiPhi, ISNULL(TongGiamGia, 0) AS TongGiamGia, ISNULL(TongGiamTruBH, 0) AS TongGiamTruBH,
    	ISNULL(TongThanhToan, 0) AS DoanhThu, ISNULL(Tienvon, 0) AS TienVon, ISNULL(LoiNhuan, 0) AS LoiNhuan, GhiChu, MaDonVi, TenDonVi, ISNULL(@STongTienHang, 0) AS STongTienHang, ISNULL(@SChietKhau,0) AS SChietKhau,
    	ISNULL(@SThue,0) AS SThue, ISNULL(@SChiPhi,0) AS SChiPhi, ISNULL(@SGiamGia,0) AS SGiamGia, ISNULL(@SGiamTruBH,0) AS SGiamTruBH, ISNULL(@SDoanhThu, 0) AS SDoanhThu, ISNULL(@STongTienVon,0) AS STongTienVon,
    	ISNULL(@SLoiNhuan,0) AS SLoiNhuan
    	FROM @tblBaoCaoDoanhThu
    	WHERE (@DoanhThuFrom IS NULL OR TongThanhToan >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR TongThanhToan <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR LoiNhuan >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR LoiNhuan <= @LoiNhuanTo)
    	ORDER BY NgayLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTaiChinh_PhanTichThuChiTheoThang_v2]
    @year [int],
    @ID_ChiNhanh [nvarchar](max),
    @loaiKH [nvarchar](max),
    @ID_NhomDoiTuong [nvarchar](max),
    @lstThuChi [nvarchar](max),
    @HachToanKD [bit],
    @LoaiTien [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
	--DECLARE @year [int] = 2023,
 --   @ID_ChiNhanh [nvarchar](max) = 'E261BA0E-6E46-47DE-AFA8-9536A9CFA584',
 --   @loaiKH [nvarchar](max) = '1,2,4',
 --   @ID_NhomDoiTuong [nvarchar](max)= '',
 --   @lstThuChi [nvarchar](max)= '1,2,3,4,5,6',
 --   @HachToanKD [bit] = 'true',
 --   @LoaiTien [nvarchar](max) = '%%';
    --	tinh ton dau ky
	DECLARE @dmDoiTuong TABLE (ID UNIQUEIDENTIFIER, LoaiDoiTuong INT, IDNhomDoiTuongs NVARCHAR(MAX));
	IF(@ID_NhomDoiTuong = '')
	BEGIN
		INSERT INTO @dmDoiTuong
		SELECT ID, LoaiDoiTuong, IDNhomDoiTuongs FROM DM_DoiTuong
	END
	ELSE
	BEGIN
		INSERT INTO @dmDoiTuong
		SELECT dt.ID, dt.LoaiDoiTuong, dt.IDNhomDoiTuongs FROM DM_DoiTuong dt
		LEFT JOIN DM_DoiTuong_Nhom dtn ON dt.ID = dtn.ID_DoiTuong
		WHERE dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong))
		GROUP BY dt.ID, dt.LoaiDoiTuong, dt.IDNhomDoiTuongs
	END
    	Declare @tmp table (ID_KhoanThuChi uniqueidentifier, KhoanMuc nvarchar(max), Thang1 float, Thang2 float, Thang3 float, Thang4 float, Thang5 float, Thang6 float, Thang7 float, Thang8 float, Thang9 float,Thang10 float,
    		Thang11 float, Thang12 float, STT int)
    		-- thu tiền
    	Insert INTO @tmp
    	SELECT
    			ID_KhoanThuChi,
    			CASE When c.LoaiThuChi = 3 then N'Thu tiền bán hàng'
    			When c.LoaiThuChi = 5 then N'Thu trả hàng nhà cung cấp'
    			When ID_KhoanThuChi is null then N'Thu mặc định'
    			else ktc.NoiDungThuChi end as KhoanMuc,
    			CASE When ThangLapHoaDon = 1 then SUM(c.TienThu) END as Thang1,
    			CASE When ThangLapHoaDon = 2 then SUM(c.TienThu) END as Thang2,
    			CASE When ThangLapHoaDon = 3 then SUM(c.TienThu) END as Thang3,
    			CASE When ThangLapHoaDon = 4 then SUM(c.TienThu) END as Thang4,
    			CASE When ThangLapHoaDon = 5 then SUM(c.TienThu) END as Thang5,
    			CASE When ThangLapHoaDon = 6 then SUM(c.TienThu) END as Thang6,
    			CASE When ThangLapHoaDon = 7 then SUM(c.TienThu) END as Thang7,
    			CASE When ThangLapHoaDon = 8 then SUM(c.TienThu) END as Thang8,
    			CASE When ThangLapHoaDon = 9 then SUM(c.TienThu) END as Thang9,
    			CASE When ThangLapHoaDon = 10 then SUM(c.TienThu) END as Thang10,
    			CASE When ThangLapHoaDon = 11 then SUM(c.TienThu) END as Thang11,
    			CASE When ThangLapHoaDon = 12 then SUM(c.TienThu) END as Thang12,
    			ROW_NUMBER() OVER(ORDER BY ktc.NoiDungThuChi) as STT
    	  FROM 
    		(
    		 SELECT 
    				b.ID_KhoanThuChi,
    			b.ThangLapHoaDon,
    				b.LoaiThuChi,
    				Case when @LoaiTien = '%1%' then SUM(b.TienMat)
    				when @LoaiTien = '%2%' then SUM(b.TienGui) else
    				SUM(b.TienMat + b.TienGui) end as TienThu
    		FROM
    		(
    			select 
    		a.ID_NhomDoiTuong,
    			a.LoaiThuChi,
    			a.ID_HoaDon,
    			a.ID_DoiTuong,
    			a.ID_KhoanThuChi,
    		a.ThangLapHoaDon,
    			a.TienMat,
    			a.TienGui,
    			a.TienThu,
    		Case when a.TienMat > 0 and TienGui = 0 then '1'  
    			when a.TienGui > 0 and TienMat = 0 then '2' 
    			when a.TienGui > 0 and TienMat > 0 then '12' end  as LoaiTien
    		From
    		(
    		select 
    			MAX(qhd.ID) as ID_HoaDon,
    				qhdct.ID_KhoanThuChi,
    			MAX(dt.ID) as ID_DoiTuong,
    				Case when qhd.HachToanKinhDoanh is null then 1 else qhd.HachToanKinhDoanh end as HachToanKinhDoanh,
    			Case when qhd.LoaiHoaDon = 11 and hd.LoaiHoaDon is null then 1 -- phiếu thu khác
    			 when (qhd.LoaiHoaDon = 12 and hd.LoaiHoaDon is null) or ((hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19) and qhd.LoaiHoaDon = 12) then 2  -- phiếu chi khác
    			 when (hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19 or hd.LoaiHoaDon = 22 or hd.LoaiHoaDon = 25) and qhd.LoaiHoaDon = 11 then 3  -- bán hàng
    			 when hd.LoaiHoaDon = 6  then 4  -- Đổi trả hàng
    			 when hd.LoaiHoaDon = 7 then 5  -- trả hàng NCC
    			 when hd.LoaiHoaDon = 4 then 6 else 7 end as LoaiThuChi, -- nhập hàng NCC
    			--Case When dtn.ID_NhomDoiTuong is null then
    			--Case When dt.LoaiDoiTuong = 1 then '00000010-0000-0000-0000-000000000010' else '30000000-0000-0000-0000-000000000003' end else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    				case when dt.IDNhomDoiTuongs is null or dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else dt.IDNhomDoiTuongs end as ID_NhomDoiTuong,
    			SUM(qhdct.TienMat) as TienMat,
    			SUM(qhdct.TienGui) as TienGui,
    			SUM(qhdct.TienThu) as TienThu,
    				MAX(DATEPART(MONTH, qhd.NgayLapHoaDon)) as ThangLapHoaDon,
    			hd.MaHoaDon
    			From Quy_HoaDon qhd 
    			inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    			left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    			left join @dmDoiTuong dt on qhdct.ID_DoiTuong = dt.ID
    			--left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    			left join Quy_KhoanThuChi ktc on qhdct.ID_KhoanThuChi = ktc.ID
    			left join DM_TaiKhoanNganHang tknh on qhdct.ID_TaiKhoanNganHang = tknh.ID
    			left join DM_NganHang nh on qhdct.ID_NganHang = nh.ID
    			where DATEPART(YEAR, qhd.NgayLapHoaDon) = @year
    			and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    			and (IIF(qhdct.ID_NhanVien is not null, 4, IIF(dt.loaidoituong IS NULL, 1, dt.LoaiDoiTuong)) in (select * from splitstring(@loaiKH)))
    			and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    			--and (qhdct.DiemThanhToan = 0 or qhdct.DiemThanhToan is null)
    				and qhd.LoaiHoaDon = 11
    			and (qhd.HachToanKinhDoanh = @HachToanKD OR @HachToanKD IS NULL)
    				AND qhdct.HinhThucThanhToan in (1,2,3,4,5)
    				--and (dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) OR @ID_NhomDoiTuong = '')
    				and (qhd.PhieuDieuChinhCongNo is null or qhd.PhieuDieuChinhCongNo !='1') 
    			Group by qhd.ID, qhd.LoaiHoaDon, hd.LoaiHoaDon,dt.LoaiDoiTuong, qhdct.ID_KhoanThuChi, 
    			qhd.HachToanKinhDoanh, qhd.MaHoaDon, qhd.NguoiNopTien, hd.MaHoaDon, dt.IDNhomDoiTuongs, qhdct.ID
    		)a
    		where a.LoaiThuChi in (select * from splitstring(@lstThuChi))
    		) b
    				where LoaiTien like @LoaiTien OR @LoaiTien = ''
    			Group by b.ID_KhoanThuChi, b.ThangLapHoaDon, b.ID_DoiTuong, b.ID_HoaDon, b.LoaiThuChi
    		) as c
    			left join Quy_KhoanThuChi ktc on c.ID_KhoanThuChi = ktc.ID
    			Group by c.ID_KhoanThuChi, c.ThangLapHoaDon, ktc.NoiDungThuChi, c.LoaiThuChi
				--select * from @tmp
    		DECLARE @dkt nvarchar(max);
    		set @dkt = (select top(1) KhoanMuc from @tmp)
    		if (@dkt is not null)
    		BEGIN
    		Insert INTO @tmp
    		select '00000010-0000-0000-0000-000000000010',
    		N'Tổng thu', SUM(Thang1)as Thang1,
    		SUM(Thang2) as Thang2,
    		SUM(Thang3) as Thang3,
    		SUM(Thang4) as Thang4,
    		SUM(Thang5) as Thang5,
    		SUM(Thang6) as Thang6,
    		SUM(Thang7) as Thang7,
    		SUM(Thang8) as Thang8,
    		SUM(Thang9) as Thang9,
    		SUM(Thang10) as Thang10,
    		SUM(Thang11) as Thang11,
    		SUM(Thang12) as Thang12,
    		MAX(STT) + 1 as STT
    		from @tmp
    		END
			--select * from @tmp
    		-- chi tiền
    		Declare @tmc table (ID_KhoanThuChi uniqueidentifier, KhoanMuc nvarchar(max), Thang1 float, Thang2 float, Thang3 float, Thang4 float, Thang5 float, Thang6 float, Thang7 float, Thang8 float, Thang9 float,Thang10 float,
    		Thang11 float, Thang12 float, STT int)
    		Insert INTO @tmc
    	SELECT
    			ID_KhoanThuChi,
    			--CASE When ID_KhoanThuChi is null then N'Chi mặc định' else ktc.NoiDungThuChi end as KhoanMuc,
    			CASE When c.LoaiThuChi = 4 then N'Chi đổi trả hàng'
    				When c.LoaiThuChi = 6 then N'Chi nhập hàng nhà cung cấp'
    				When ID_KhoanThuChi is null then N'Chi mặc định'
    				else ktc.NoiDungThuChi end as KhoanMuc,
    			CASE When ThangLapHoaDon = 1 then SUM(c.TienThu) END as Thang1,
    			CASE When ThangLapHoaDon = 2 then SUM(c.TienThu) END as Thang2,
    			CASE When ThangLapHoaDon = 3 then SUM(c.TienThu) END as Thang3,
    			CASE When ThangLapHoaDon = 4 then SUM(c.TienThu) END as Thang4,
    			CASE When ThangLapHoaDon = 5 then SUM(c.TienThu) END as Thang5,
    			CASE When ThangLapHoaDon = 6 then SUM(c.TienThu) END as Thang6,
    			CASE When ThangLapHoaDon = 7 then SUM(c.TienThu) END as Thang7,
    			CASE When ThangLapHoaDon = 8 then SUM(c.TienThu) END as Thang8,
    			CASE When ThangLapHoaDon = 9 then SUM(c.TienThu) END as Thang9,
    			CASE When ThangLapHoaDon = 10 then SUM(c.TienThu) END as Thang10,
    			CASE When ThangLapHoaDon = 11 then SUM(c.TienThu) END as Thang11,
    			CASE When ThangLapHoaDon = 12 then SUM(c.TienThu) END as Thang12,
    			ROW_NUMBER() OVER(ORDER BY ktc.NoiDungThuChi ASC) + (select MAX(STT) from @tmp) as STT
    	  FROM 
    		(
    		 SELECT 
    				b.ID_KhoanThuChi,
    			b.ThangLapHoaDon,
    				b.LoaiThuChi,
    				Case when @LoaiTien = '%1%' then SUM(b.TienMat)
    				when @LoaiTien = '%2%' then SUM(b.TienGui) else
    				SUM(b.TienMat + b.TienGui) end as TienThu
    		FROM
    		(
    				select 
    			a.ID_NhomDoiTuong,
    				a.LoaiThuChi,
    				a.ID_HoaDon,
    				a.ID_DoiTuong,
    				a.ID_KhoanThuChi,
    			a.ThangLapHoaDon,
    				a.TienMat,
    				a.TienGui,
    				a.TienThu,
    			Case when a.TienMat > 0 and TienGui = 0 then '1'  
    			 when a.TienGui > 0 and TienMat = 0 then '2' 
    			 when a.TienGui > 0 and TienMat > 0 then '12' end  as LoaiTien
    		From
    		(
    		select
    			MAX(qhd.ID) as ID_HoaDon,
    				qhdct.ID_KhoanThuChi,
    			MAX(dt.ID) as ID_DoiTuong,
    			Case when qhd.HachToanKinhDoanh is null then 1 else qhd.HachToanKinhDoanh end as HachToanKinhDoanh,
    			Case when qhd.LoaiHoaDon = 11 and hd.LoaiHoaDon is null then 1 else -- phiếu thu khác
    			Case when (qhd.LoaiHoaDon = 12 and hd.LoaiHoaDon is null) or ((hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19) and qhd.LoaiHoaDon = 12) then 2 else -- phiếu chi khác
    			Case when (hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19 or hd.LoaiHoaDon = 22 or hd.LoaiHoaDon = 25) and qhd.LoaiHoaDon = 11 then 3 else -- bán hàng
    			Case when hd.LoaiHoaDon = 6  then 4 else -- Đổi trả hàng
    			Case when hd.LoaiHoaDon = 7 then 5 else -- trả hàng NCC
    			Case when hd.LoaiHoaDon = 4 then 6 else '' end end end end end end as LoaiThuChi, -- nhập hàng NCC
    			--Case When dtn.ID_NhomDoiTuong is null then
    			--Case When dt.LoaiDoiTuong = 1 then '00000010-0000-0000-0000-000000000010' else '30000000-0000-0000-0000-000000000003' end else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
				case when dt.IDNhomDoiTuongs is null or dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else dt.IDNhomDoiTuongs end as ID_NhomDoiTuong,
    			Case when qhd.NguoiNopTien is null or qhd.NguoiNopTien = '' then N'Khách lẻ' else qhd.NguoiNopTien end as TenNguoiNop,
    			SUM(qhdct.TienMat) as TienMat,
    			SUM(qhdct.TienGui) as TienGui,
    			SUM(qhdct.TienThu) as TienThu,
    				MAX(DATEPART(MONTH, qhd.NgayLapHoaDon)) as ThangLapHoaDon
    			From Quy_HoaDon qhd 
    			inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    			left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    			left join @dmDoiTuong dt on qhdct.ID_DoiTuong = dt.ID
    			--left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    			left join Quy_KhoanThuChi ktc on qhdct.ID_KhoanThuChi = ktc.ID
    			left join DM_TaiKhoanNganHang tknh on qhdct.ID_TaiKhoanNganHang = tknh.ID
    			left join DM_NganHang nh on qhdct.ID_NganHang = nh.ID
    			where DATEPART(YEAR, qhd.NgayLapHoaDon) = @year
    			and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    			and (IIF(qhdct.ID_NhanVien is not null, 4, IIF(dt.loaidoituong IS NULL, 1, dt.LoaiDoiTuong)) in (select * from splitstring(@loaiKH)))
    			and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    			and (qhdct.DiemThanhToan is null OR qhdct.DiemThanhToan = 0)
    				and qhd.LoaiHoaDon = 12
    			and (qhd.HachToanKinhDoanh = @HachToanKD OR @HachToanKD IS NULL)
    				AND qhdct.HinhThucThanhToan != 6
    				--and (dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) OR @ID_NhomDoiTuong = '')
    				and (qhd.PhieuDieuChinhCongNo is null or qhd.PhieuDieuChinhCongNo !='1') 
    			Group by qhd.ID,qhd.LoaiHoaDon, hd.LoaiHoaDon, dt.LoaiDoiTuong, qhdct.ID_KhoanThuChi,
    			 qhd.HachToanKinhDoanh, qhd.MaHoaDon, qhd.NguoiNopTien, hd.MaHoaDon, dt.IDNhomDoiTuongs ,qhdct.ID
    		)a
    		where a.LoaiThuChi in (select * from splitstring(@lstThuChi))
    		) b
    				where LoaiTien like @LoaiTien OR @LoaiTien = ''
    			Group by b.ID_KhoanThuChi, b.ThangLapHoaDon, b.ID_DoiTuong, b.ID_HoaDon, b.LoaiThuChi
    		) as c
    			left join Quy_KhoanThuChi ktc on c.ID_KhoanThuChi = ktc.ID
    			Group by c.ID_KhoanThuChi, c.ThangLapHoaDon, ktc.NoiDungThuChi, c.LoaiThuChi
    		DECLARE @dk nvarchar(max);
    		set @dk = (select top(1) KhoanMuc from @tmc)
    		if (@dk is not null)
    		BEGIN
    		Insert INTO @tmp
    			select *
    			from @tmc
    		Insert INTO @tmp
    			select 
    			'00000030-0000-0000-0000-000000000030',
    			N'Tổng chi', 
    			SUM(Thang1)as Thang1,
    			SUM(Thang2) as Thang2,
    			SUM(Thang3) as Thang3,
    			SUM(Thang4) as Thang4,
    			SUM(Thang5) as Thang5,
    			SUM(Thang6) as Thang6,
    			SUM(Thang7) as Thang7,
    			SUM(Thang8) as Thang8,
    			SUM(Thang9) as Thang9,
    			SUM(Thang10) as Thang10,
    			SUM(Thang11) as Thang11,
    			SUM(Thang12) as Thang12,
    			MAX(STT) + 1 as STT
    			from @tmc
    		END
    			select max(ID_KhoanThuChi) as ID_KhoanThuChi, -- deu chi tien nhaphang, nhưng ID_KhoanThuChi # nhau --> thi bi douple, nen chi group tho KhoanMua va lay max (ID_KhoanThuChi)
    			KhoanMuc, 
    			CAST(ROUND(SUM(Thang1), 0) as float) as Thang1,
    			CAST(ROUND(SUM(Thang2), 0) as float) as Thang2,
    			CAST(ROUND(SUM(Thang3), 0) as float) as Thang3,
    			CAST(ROUND(SUM(Thang4), 0) as float) as Thang4,
    			CAST(ROUND(SUM(Thang5), 0) as float) as Thang5,
    			CAST(ROUND(SUM(Thang6), 0) as float) as Thang6,
    			CAST(ROUND(SUM(Thang7), 0) as float) as Thang7,
    			CAST(ROUND(SUM(Thang8), 0) as float) as Thang8,
    			CAST(ROUND(SUM(Thang9), 0) as float) as Thang9,
    			CAST(ROUND(SUM(Thang10), 0) as float) as Thang10,
    			CAST(ROUND(SUM(Thang11), 0) as float) as Thang11,
    			CAST(ROUND(SUM(Thang12), 0) as float) as Thang12,
    			ISNULL(SUM(Thang1),0) + ISNULL(SUM(Thang2),0) + ISNULL(SUM(Thang3),0) + ISNULL(SUM(Thang4),0) + ISNULL(SUM(Thang5),0) + ISNULL(SUM(Thang6),0) + ISNULL(SUM(Thang7),0) + ISNULL(SUM(Thang8),0) + 
    			ISNULL(SUM(Thang9),0) + ISNULL(SUM(Thang10),0) + ISNULL(SUM(Thang11),0) + ISNULL(SUM(Thang12),0) as TongCong
    			from @tmp
    			GROUP BY  KhoanMuc
    			order by MAX(STT)
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListTonTheoLoHangHoa]
    @timeEnd [datetime],
    @ID_ChiNhanh [uniqueidentifier],
    @ID_HangHoa [uniqueidentifier]
AS
BEGIN

	SELECT dmlo.ID as ID_LoHang, dmlo.MaLoHang, dmlo.NgaySanXuat, dmlo.NgayHetHan, 
		ROUND(ISNULL(hhtonkho.TonKho, 0),3) as TonKho
	FROM
	DM_LoHang dmlo
	LEFT JOIN DonViQuiDoi dvqd on dmlo.ID_HangHoa = dvqd.ID_HangHoa
	LEFT JOIN DM_HangHoa_TonKho hhtonkho ON dvqd.ID = hhtonkho.ID_DonViQuyDoi AND hhtonkho.ID_DonVi = @ID_ChiNhanh AND hhtonkho.ID_LoHang = dmlo.ID
	WHERE dvqd.ID_HangHoa = @ID_HangHoa and dvqd.LaDonViChuan = 1
END");
        }
        
        public override void Down()
        {
			Sql("DROP FUNCTION [dbo].[fnGetAllHangHoa_NeedUpdateTonKhoGiaVon]");
			DropStoredProcedure("[dbo].[GetQuyen_ByIDNguoiDung]");
        }
    }
}
