﻿namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20210614 : DbMigration
    {
        public override void Up()
        {
			CreateStoredProcedure(name: "[dbo].[CTHD_GetDichVubyDinhLuong]", parametersAction: p => new
			{
				ID_HoaDon = p.Guid(),
				ID_DonViQuiDoi = p.Guid(),
				ID_LoHang = p.Guid()
			}, body: @"SET NOCOUNT ON;	

			select 
				ctsc.ID_ChiTietGoiDV, --- ~ id of thanhphan
				ctsc.ID_ChiTietDinhLuong, --- ~ id_quidoi of dichvu
				ctsc.SoLuong,
				ctsc.SoLuong - isnull(ctxk.SoLuongXuat,0) as SoLuongConLai,
				ctsc.ID_DonViQuiDoi, ---- ~ id hanghoa,
				ctsc.ID_LoHang,
				hh.QuanLyTheoLoHang,
				hh.LaHangHoa,
				hh.DichVuTheoGio,
				hh.DuocTichDiem,
				qd.GiaBan,
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,
				ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
				CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
				hh.LaHangHoa, hh.TenHangHoa, 
				hh.ID_NhomHang as ID_NhomHangHoa, ISNULL(hh.GhiChu,'') as GhiChuHH,
				ctsc.IDChiTietDichVu ---- ~ id chitietHD of dichvu
			from 
			(
				--- ct hoadon suachua
				select 
					cttp.ID as ID_ChiTietGoiDV,
					isnull(ctdv.ID_DichVu,cttp.ID_DonViQuiDoi) as ID_ChiTietDinhLuong, -- id_hanghoa/id_dichvu
					cttp.SoLuong,
					cttp.ID_DonViQuiDoi,
					cttp.ID_LoHang,
					cttp.ID_ChiTietDinhLuong AS IDChiTietDichVu --- used to xuất kho hàng ngoài
				from BH_HoaDon_ChiTiet cttp
				left join
				(
					select ctm.ID_DonViQuiDoi as ID_DichVu, ctm.ID
					from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
				) ctdv on cttp.ID_ChiTietDinhLuong = ctdv.ID
				where cttp.ID_DonViQuiDoi = @ID_DonViQuiDoi
				and ((cttp.ID_LoHang = @ID_LoHang) or (cttp.ID_LoHang is null and @ID_LoHang is null))
				and cttp.ID_HoaDon= @ID_HoaDon
			) ctsc
			left join
			(
					---- ct xuatkho 
				select sum(ct.SoLuong) as SoLuongXuat,
					ct.ID_ChiTietGoiDV
				from BH_HoaDon_ChiTiet ct
				join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
				where hd.ChoThanhToan='0' and hd.LoaiHoaDon=8
				and hd.ID_HoaDon= @ID_HoaDon
				and ct.ID_DonViQuiDoi= @ID_DonViQuiDoi 
				and ((ct.ID_LoHang = @ID_LoHang) or (ct.ID_LoHang is null and @ID_LoHang is null))
				group by ct.ID_ChiTietGoiDV
			) ctxk on ctsc.ID_ChiTietGoiDV = ctxk.ID_ChiTietGoiDV
			join DonViQuiDoi qd on ctsc.ID_ChiTietDinhLuong= qd.ID
			join DM_HangHoa hh on qd.ID_HangHoa= hh.ID	
			left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID");

			CreateStoredProcedure(name: "[dbo].[GetCTHDSuaChua_afterXuatKho]", parametersAction: p => new
			{
				ID_HoaDon = p.String()
			}, body: @"set nocount on

	--- get cthd of hdsc
	select cthd.*,
		cthd.SoLuong * isnull(gv.GiaVon,0) as ThanhTien,
		isnull(gv.GiaVon,0) as GiaVon,
		isnull(tk.TonKho,0) as TonKho,
		isnull(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
		lo.NgaySanXuat, lo.NgayHetHan, isnull(lo.MaLoHang,'') as MaLoHang, 
		hh.QuanLyTheoLoHang,
		hh.LaHangHoa,
		hh.DichVuTheoGio,
		hh.DuocTichDiem,
		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
		qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
		hh.LaHangHoa, hh.TenHangHoa, CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach, hh.ID_NhomHang as ID_NhomHangHoa, ISNULL(hh.GhiChu,'') as GhiChuHH
	from
	(
			select 
				 ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon,
				 max(ctsc.ID_DonVi) as ID_DonVi,
				 sum(SoLuong) as SoLuongMua,
				 sum(SoLuongXuat) as SoLuongXuat,
				 sum(SoLuong) - isnull(sum(SoLuongXuat),0) as SoLuong
			from
			(
			select sum(ct.SoLuong) as SoLuong,
				0 as SoLuongXuat,
				ct.ID_DonViQuiDoi,
				ct.ID_LoHang,
				ct.ID_HoaDon,
				hd.ID_DonVi
			from BH_HoaDon_ChiTiet ct
			join BH_HoaDon hd on hd.ID= ct.ID_HoaDon
			where ct.ID_HoaDon= @ID_HoaDon
			and (ct.ID_ChiTietDinhLuong != ct.ID or ct.ID_ChiTietDinhLuong is null)
			group by ct.ID_DonViQuiDoi, ct.ID_LoHang,ct.ID_HoaDon,hd.ID_DonVi

			union all
			-- get cthd daxuat kho
			select 0 as SoLuong,
				sum(ct.SoLuong) as SoLuongXuat,
				ct.ID_DonViQuiDoi,
				ct.ID_LoHang,
				@ID_HoaDon as ID_HoaDon,
				'00000000-0000-0000-0000-000000000000' as ID_DonVi
			from BH_HoaDon_ChiTiet ct
			join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
			where hd.ID_HoaDon= @ID_HoaDon
			and hd.ChoThanhToan='0'
			group by ct.ID_DonViQuiDoi, ct.ID_LoHang
			)ctsc
			group by ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon
	) cthd
	join DonViQuiDoi qd on cthd.ID_DonViQuiDoi= qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
	left join DM_LoHang lo on cthd.ID_LoHang= lo.ID and hh.ID= lo.ID_HangHoa
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
	left join DM_HangHoa_TonKho tk on (qd.ID = tk.ID_DonViQuyDoi and (lo.ID = tk.ID_LoHang or lo.ID is null) and  tk.ID_DonVi = cthd.ID_DonVi)
	left join DM_GiaVon gv on (qd.ID = gv.ID_DonViQuiDoi and (lo.ID = gv.ID_LoHang or lo.ID is null) and gv.ID_DonVi = cthd.ID_DonVi) -- lay giavon hientai --> xuatkho gara tu hdsc
	where hh.LaHangHoa= 1");

			CreateStoredProcedure(name: "[dbo].[HDSC_GetChiTietXuatKho]", parametersAction: p => new
			{
				ID_HoaDon = p.Guid(),
				IDChiTietHD = p.Guid(),
				LoaiHang = p.Int()
			}, body: @"SET NOCOUNT ON;

	if	@LoaiHang = 1 -- hanghoa
		begin
		select 
			qd.MaHangHoa, qd.TenDonViTinh,
			hh.TenHangHoa,
			lo.MaLoHang,
			pxk.SoLuong,
			round(pxk.GiaVon * pxk.SoLuong,3) as GiaVon,
			pxk.MaHoaDon,
			pxk.NgayLapHoaDon,
			pxk.GhiChu
		from(
			select 
				hd.MaHoaDon,
				hd.NgayLapHoaDon,
				ctxk.ID_DonViQuiDoi,
				ctxk.ID_LoHang,
				ctxk.SoLuong,
				ctxk.SoLuong * ctxk.GiaVon as GiaVon,
				ctxk.GhiChu
			from BH_HoaDon_ChiTiet ctxk
			join BH_HoaDon hd on ctxk.ID_HoaDon= hd.ID
			where ctxk.ID_ChiTietGoiDV = @IDChiTietHD		
			and hd.ChoThanhToan='0'
		) pxk
		join DonViQuiDoi qd on pxk.ID_DonViQuiDoi= qd.ID
		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
		left join DM_LoHang lo on pxk.ID_LoHang= lo.ID

		end
	else
	begin

			select 
				hh.TenHangHoa,
				qd.MaHangHoa, qd.TenDonViTinh, qd.ThuocTinhGiaTri,
				isnull(lo.MaLoHang,'') as MaLoHang,
				tpdl.SoLuongDinhLuong_BanDau,
				round(tpdl.GiaTriDinhLuong_BanDau,3) as GiaTriDinhLuong_BanDau ,
				tpdl.MaHoaDon,
				tpdl.NgayLapHoaDon	,
				tpdl.SoLuongXuat as SoLuong,
				round(tpdl.GiaTriXuat,3) as GiaVon,
				tpdl.GhiChu,
				tpdl.LaDinhLuongBoSung
			from
			(
						---- get tpdl ban dau
						select 	
							ctxk.MaHoaDon,
							ctxk.NgayLapHoaDon,
							ct.SoLuong as SoLuongDinhLuong_BanDau,
							ct.SoLuong * ct.GiaVon as GiaTriDinhLuong_BanDau,
							ct.ID_DonViQuiDoi, 
							ct.ID_LoHang,
							isnull(ctxk.SoLuongXuat,0) as SoLuongXuat,
							isnull(ctxk.GiaTriXuat,0) as GiaTriXuat,
							isnull(ctxk.GhiChu,'') as GhiChu,
							0 as LaDinhLuongBoSung
						from BH_HoaDon_ChiTiet ct
						left join
						(
							---- get tpdl when xuatkho (ID_ChiTietGoiDV la hanghoa)
							select 
				
									hd.MaHoaDon,
									hd.NgayLapHoaDon	,
									ct.SoLuong as SoLuongXuat,
									round(ct.SoLuong * ct.GiaVon,3) as GiaTriXuat,
									ct.GhiChu,
									ct.ID_ChiTietGoiDV
							from BH_HoaDon_ChiTiet ct
							join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
							where hd.ChoThanhToan='0'
						) ctxk on ct.ID= ctxk.ID_ChiTietGoiDV
						where ct.ID_ChiTietDinhLuong= @IDChiTietHD
						and ct.ID != ct.ID_ChiTietDinhLuong				

						---- get dinhluong them vao khi tao phieu xuatkho (ID_ChiTietGoiDV la dichvu)
						union all

						select 
							hd.MaHoaDon,
							hd.NgayLapHoaDon,
							ct.SoLuong as SoLuongDinhLuong_BanDau,
							ct.SoLuong * ct.GiaVon as GiaTriDinhLuong_BanDau,
							ct.ID_DonViQuiDoi, 
							ct.ID_LoHang,
							isnull(ctxk.SoLuongXuat,0) as SoLuongXuat,
							isnull(ctxk.GiaTriXuat,0) as GiaTriXuat,
							isnull(ct.GhiChu,'') as GhiChu,
							1 as LaDinhLuongBoSung
						from BH_HoaDon_ChiTiet ct
						join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
						left join
						(
							---- sum soluongxuat cua chinh no
							select 
									sum(ct.SoLuong) as SoLuongXuat,
									sum(round(ct.SoLuong * ct.GiaVon,3)) as GiaTriXuat,
									ct.ID_DonViQuiDoi
							from BH_HoaDon_ChiTiet ct
							join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
							where hd.ChoThanhToan='0'
							and hd.LoaiHoaDon= 8 
							and ct.ID_ChiTietGoiDV= @IDChiTietHD
							group by ct.ID_DonViQuiDoi
						) ctxk on ct.ID_DonViQuiDoi= ctxk.ID_DonViQuiDoi
						where hd.ChoThanhToan='0'
						and hd.LoaiHoaDon= 8 
						and ct.ID_ChiTietGoiDV= @IDChiTietHD

			) tpdl
			join DonViQuiDoi qd on qd.ID= tpdl.ID_DonViQuiDoi
			join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
			left join DM_LoHang lo on tpdl.ID_LoHang= lo.ID
			order by tpdl.NgayLapHoaDon desc
		
	end");

			CreateStoredProcedure(name: "[dbo].[InsertChietKhauTraHang_TheoThucThu]", parametersAction: p => new
			{
				ID_HoaDonTra = p.Guid(),
				ID_PhieuChi = p.Guid()
			}, body: @"SET NOCOUNT ON;

	declare @ID_DonVi uniqueidentifier, @ID_HoaDonGoc uniqueidentifier , @TongChi float
	select @ID_DonVi= ID_DonVi, @ID_HoaDonGoc = ID_HoaDon from BH_HoaDon where ID= @ID_HoaDonTra
	select @TongChi = TongTienThu from Quy_HoaDon where ID= @ID_PhieuChi


		---- check xem co cai dat chiet khau TraHang khong
		declare @count_CKTraHang int   	
    	select @count_CKTraHang = count(hd.ID)
    	from ChietKhauMacDinh_HoaDon hd
    	where hd.ID_DonVi like @ID_DonVi
    	and hd.TrangThai !='0' and  hd.ChungTuApDung like '%6%'

		if	@count_CKTraHang > 0
    		begin		  		 			
    		
			insert into BH_NhanVienThucHien (ID, ID_NhanVien,  PT_ChietKhau, TinhChietKhauTheo, HeSo,ID_HoaDon,ThucHien_TuVan, TheoYeuCau, TienChietKhau, ID_QuyHoaDon)									
				select 
					NewID() as ID,
					ID_NhanVien,
					PT_ChietKhau,
					TinhChietKhauTheo,
					HeSo,
					@ID_HoaDonTra,
					ThucHien_TuVan,
					TheoYeuCau,
					(@TongChi * PT_ChietKhau *HeSo)/100 as TienChietKhau	,
					@ID_PhieuChi
				from(
					select 
						th.*, ROW_NUMBER() over (partition by th.ID_NhanVien order by th.ID_NhanVien, TienChietKhau desc) as Rn
					from BH_NhanVienThucHien th
					where ID_HoaDon like @ID_HoaDonGoc
					and TinhChietKhauTheo =1 -- thucthu: neu thanhtoan nhieulan, lay ck lon nhat
				) a where Rn= 1
			
    		end");

			CreateStoredProcedure(name: "[dbo].[UpdateIDCTNew_forCTOld]", parametersAction: p => new
			{
				Pair_IDNewIDOld = p.String()
			}, body: @"SET NOCOUNT ON;

	declare @PairNewOld nvarchar(max)
	declare _cur Cursor
	for
	 select Name from dbo.splitstringByChar(@Pair_IDNewIdOld,';')

	 open _cur
	 fetch next from _cur into @PairNewOld
	 while @@FETCH_STATUS = 0  
	 begin	
		if @PairNewOld!=''
		begin
			select cast (name as uniqueidentifier) as ID, 
			row_number() over( order by (select 1)) as Rn 
			into #temp
			from dbo.splitstring(@PairNewOld)

			update ct set ct.ID_ChiTietGoiDV = (select top 1 ID from #temp where Rn= 1) -- idnew
			from BH_HoaDon_ChiTiet ct
			where ct.ID_ChiTietGoiDV = (select top 1 ID from #temp where Rn= 2) --idold

			drop table #temp
		end	

		FETCH NEXT FROM _cur INTO @PairNewOld 
	 end
	 close _cur
	 deallocate _cur");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTongQuan_BieuDoDoanhThuToDay]
    @timeStart [datetime],
    @timeEnd [datetime],
	@ID_NguoiDung [uniqueidentifier],
	@ID_DonVi nvarchar (max)
AS
BEGIN
	 DECLARE @LaAdmin as nvarchar
    	Set @LaAdmin = (Select nd.LaAdmin From HT_NguoiDung nd	where nd.ID = @ID_NguoiDung)
	 IF(@LaAdmin = 1)
	 BEGIN
		SELECT 
			a.NgayLapHoaDon,
			a.TenChiNhanh,
			CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien
			FROM
			(
				-- tongmua
    			SELECT
    			hdb.ID as ID_HoaDon,
				DAY(hdb.NgayLapHoaDon) as NgayLapHoaDon,
				dv.TenDonVi as TenChiNhanh,
    			hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien 
    			FROM
    			BH_HoaDon hdb
				join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    			where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    			and hdb.ChoThanhToan = 0
    			and (hdb.LoaiHoaDon = 1 Or hdb.LoaiHoaDon = 19)
				and hdb.ID_DonVi in (select * from splitstring(@ID_DonVi))

				union all
				-- tongtra
				SELECT
    			hdb.ID as ID_HoaDon,
				DAY(hdb.NgayLapHoaDon) as NgayLapHoaDon,
				dv.TenDonVi as TenChiNhanh,
    			- hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien 
    			FROM
    			BH_HoaDon hdb
				join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    			where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    			and hdb.ChoThanhToan = 0
    			and hdb.LoaiHoaDon = 6
				and hdb.ID_DonVi in (select * from splitstring(@ID_DonVi))				
			) a
    		GROUP BY a.NgayLapHoaDon, a.TenChiNhanh
			ORDER BY NgayLapHoaDon
	END
	ELSE
	BEGIN
		SELECT 
			a.NgayLapHoaDon,
			a.TenChiNhanh,
			CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien
			FROM
			(
				-- tongmua
    			SELECT
    			hdb.ID as ID_HoaDon,
				DAY(hdb.NgayLapHoaDon) as NgayLapHoaDon,
				dv.TenDonVi as TenChiNhanh,
    			hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien
    			FROM
    			BH_HoaDon hdb
				join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    			where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    			and hdb.ChoThanhToan = 0
    			and hdb.ID_DonVi in (select ct.ID_DonVi from HT_NguoiDung nd 
									join NS_NhanVien nv on nv.ID = nd.ID_NhanVien 
									join NS_QuaTrinhCongTac ct on ct.ID_NhanVien = nv.ID 
									 where nd.ID = @ID_NguoiDung)
    			and (hdb.LoaiHoaDon = 1 Or hdb.LoaiHoaDon = 19)
				and hdb.ID_DonVi in (select * from splitstring(@ID_DonVi))

				union all
				-- tongtra
    			SELECT
    			hdb.ID as ID_HoaDon,
				DAY(hdb.NgayLapHoaDon) as NgayLapHoaDon,
				dv.TenDonVi as TenChiNhanh,
    			-hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien
    			FROM
    			BH_HoaDon hdb
				join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    			where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    			and hdb.ChoThanhToan = 0
    			and hdb.ID_DonVi in (select ct.ID_DonVi from HT_NguoiDung nd 
									join NS_NhanVien nv on nv.ID = nd.ID_NhanVien 
									join NS_QuaTrinhCongTac ct on ct.ID_NhanVien = nv.ID 
									 where nd.ID = @ID_NguoiDung)
    			and hdb.LoaiHoaDon = 6
				and hdb.ID_DonVi in (select * from splitstring(@ID_DonVi))
			) a
    		GROUP BY a.NgayLapHoaDon, a.TenChiNhanh
			ORDER BY NgayLapHoaDon
	END

END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTongQuan_DoanhThuChiNhanh]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	DECLARE @LaAdmin as nvarchar
    	Set @LaAdmin = (Select nd.LaAdmin From HT_NguoiDung nd	where nd.ID = @ID_NguoiDung)
	 IF(@LaAdmin = 1)
	 BEGIN
		SELECT 
		a.TenChiNhanh,
		CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien
		FROM
		(
			-- tongmua
    		SELECT
			dv.TenDonVi as TenChiNhanh,
    		hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien
    		FROM
    		BH_HoaDon hdb
			join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    		where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    		and hdb.ChoThanhToan = 0
    		and (hdb.LoaiHoaDon = 1 Or hdb.LoaiHoaDon = 19)

			union all
			-- tongtra
    		SELECT
			dv.TenDonVi as TenChiNhanh,
    		-hdb.PhaiThanhToan  - isnull(hdb.TongTienThue,0)as ThanhTien
    		FROM
    		BH_HoaDon hdb
			join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    		where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    		and hdb.ChoThanhToan = 0
    		and hdb.LoaiHoaDon = 6
		) a
    	GROUP BY a.TenChiNhanh
	END
	ELSE
	BEGIN
		SELECT 
		a.TenChiNhanh,
		CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien
		FROM
		(
			--tongban
    		SELECT
			dv.TenDonVi as TenChiNhanh,
    		hdb.PhaiThanhToan - isnull(hdb.TongTienThue,0) as ThanhTien
    		FROM
    		BH_HoaDon hdb
			join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    		where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    		and hdb.ChoThanhToan = 0
			and hdb.ID_DonVi in (select ct.ID_DonVi from HT_NguoiDung nd 
									join NS_NhanVien nv on nv.ID = nd.ID_NhanVien 
									join NS_QuaTrinhCongTac ct on ct.ID_NhanVien = nv.ID 
									 where nd.ID = @ID_NguoiDung)
    		and (hdb.LoaiHoaDon = 1 Or hdb.LoaiHoaDon = 19)

			union all
			-- tongtra
			SELECT
			dv.TenDonVi as TenChiNhanh,
    		- hdb.PhaiThanhToan  - isnull(hdb.TongTienThue,0)as ThanhTien
    		FROM
    		BH_HoaDon hdb
			join DM_DonVi dv on hdb.ID_DonVi = dv.ID
    		where hdb.NgayLapHoaDon >= @timeStart and hdb.NgayLapHoaDon < @timeEnd
    		and hdb.ChoThanhToan = 0
			and hdb.ID_DonVi in (select ct.ID_DonVi from HT_NguoiDung nd 
									join NS_NhanVien nv on nv.ID = nd.ID_NhanVien 
									join NS_QuaTrinhCongTac ct on ct.ID_NhanVien = nv.ID 
									 where nd.ID = @ID_NguoiDung)
    		and hdb.LoaiHoaDon = 6
		) a
    	GROUP BY a.TenChiNhanh
	END
END");

			Sql(@"ALTER PROCEDURE [dbo].[getListHangHoaLoHang_EnTer]
    @MaHH [nvarchar](max),
    @ID_ChiNhanh [uniqueidentifier]
AS
BEGIN
    	Select TOP(40)
		dvqd1.ID as ID_DonViQuiDoi,
    	dhh1.ID,
		dvqd1.MaHangHoa,
    	dhh1.TenHangHoa,
		dvqd1.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    	dvqd1.TenDonViTinh,
		dhh1.QuanLyTheoLoHang,
		dvqd1.TyLeChuyenDoi,
		dhh1.LaHangHoa,
		Case When gv.ID is null then 0 else CAST(ROUND(( gv.GiaVon), 0) as float) end as GiaVon,
		dvqd1.GiaBan,
		dvqd1.GiaNhap,
		ISNULL(hhtonkho.TonKho,0) as TonKho,
		ISNULL(an.URLAnh,'/Content/images/iconbepp18.9/gg-37.png') as SrcImage,
		Case when lh1.ID is null then null else lh1.ID end as ID_LoHang,
		lh1.MaLoHang,
    	lh1.NgaySanXuat,
		lh1.NgayHetHan,
		case when ISNULL(dhh1.QuyCach,0) = 0 then dvqd1.TyLeChuyenDoi else dhh1.QuyCach * dvqd1.TyLeChuyenDoi end as QuyCach
    	from
    	DonViQuiDoi dvqd1
    	left join DM_HangHoa dhh1 on dvqd1.ID_HangHoa = dhh1.ID
		LEFT join DM_HangHoa_Anh an on (dvqd1.ID_HangHoa = an.ID_HangHoa and (an.sothutu = 1 or an.ID is null))
    	left join DM_LoHang lh1 on dvqd1.ID_HangHoa = lh1.ID_HangHoa and (lh1.TrangThai = 1 or lh1.TrangThai is null)
		left join DM_GiaVon gv on dvqd1.ID = gv.ID_DonViQuiDoi and (lh1.ID = gv.ID_LoHang or lh1.ID is null) and gv.ID_DonVi = @ID_ChiNhanh
		left join DM_HangHoa_TonKho hhtonkho on dvqd1.ID = hhtonkho.ID_DonViQuyDoi and (hhtonkho.ID_LoHang = lh1.ID or lh1.ID is null) and hhtonkho.ID_DonVi = @ID_ChiNhanh
		where dvqd1.Xoa = 0 and dvqd1.MaHangHoa = @MaHH
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListPhieuNhapXuatKhoByIDPhieuTiepNhan]
    @IDPhieuTiepNhan [uniqueidentifier],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	IF(@PageSize != 0)
    	BEGIN
    		with data_cte
    		as
    		(select pxk.ID, pxk.LoaiHoaDon, pxk.MaHoaDon, pxk.NgayLapHoaDon, hdsc.ID AS ID_HoaDonSuaChua, hdsc.MaHoaDon AS HoaDonSuaChua, hdsc.ChoThanhToan AS TrangThaiHoaDonSuaChua, SUM(pxkct.SoLuong) AS SoLuong, SUM(pxkct.ThanhTien) AS GiaTri from BH_HoaDon pxk
    		LEFT JOIN BH_HoaDon hdsc ON pxk.ID_HoaDon = hdsc.ID
    		INNER JOIN BH_HoaDon_ChiTiet pxkct ON pxk.ID = pxkct.ID_HoaDon
    		where pxk.ID_PhieuTiepNhan = @IDPhieuTiepNhan
    		AND pxk.LoaiHoaDon = '8' AND pxk.ChoThanhToan = 0
    		GROUP BY pxk.ID, pxk.MaHoaDon, pxk.NgayLapHoaDon, hdsc.MaHoaDon, pxk.LoaiHoaDon, hdsc.ID, hdsc.ChoThanhToan),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    			from data_cte
    		)
    
    		SELECT * FROM data_cte dt
    		CROSS JOIN count_cte cte
    		order by dt.NgayLapHoaDon desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
    	END
    	ELSE
    	BEGIN
    		with data_cte
    		as
    		(select pxk.ID, pxk.LoaiHoaDon, pxk.MaHoaDon, pxk.NgayLapHoaDon, hdsc.ID AS ID_HoaDonSuaChua, hdsc.MaHoaDon AS HoaDonSuaChua, hdsc.ChoThanhToan AS TrangThaiHoaDonSuaChua, SUM(pxkct.SoLuong) AS SoLuong, SUM(pxkct.ThanhTien) AS GiaTri from BH_HoaDon pxk
    		LEFT JOIN BH_HoaDon hdsc ON pxk.ID_HoaDon = hdsc.ID
    		INNER JOIN BH_HoaDon_ChiTiet pxkct ON pxk.ID = pxkct.ID_HoaDon
    		where pxk.ID_PhieuTiepNhan = @IDPhieuTiepNhan
    		AND pxk.LoaiHoaDon = '8' AND pxk.ChoThanhToan = 0
    		GROUP BY pxk.ID, pxk.MaHoaDon, pxk.NgayLapHoaDon, hdsc.MaHoaDon, pxk.LoaiHoaDon, hdsc.ID, hdsc.ChoThanhToan),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    			from data_cte
    		)
    
    		SELECT dt.*, 0 AS TotalRow, CAST(0 AS FLOAT) AS TotalPage FROM data_cte dt
    		order by dt.NgayLapHoaDon desc
    	END
END");

            Sql(@"ALTER PROCEDURE [dbo].[ReportDiscountProduct_General]
    @ID_ChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [nvarchar](max),
    @TextSearch [nvarchar](max),
	@LoaiChungTus [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status_ColumHide [int],
    @StatusInvoice [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    set nocount on;
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

		declare @tblChungTu table (LoaiChungTu int)
    	insert into @tblChungTu
    	select Name from dbo.splitstring(@LoaiChungTus)
    
    	declare @nguoitao nvarchar(100) = (select top 1 taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanhs,'BCCKHangHoa_XemDS_PhongBan','BCCKHangHoa_XemDS_HeThong');
    
    --	select * from @tblNhanVien
    
    
    	set @DateTo = DATEADD(day,1,@DateTo);
    
    	with data_cte
    	as (
    
    	select nv.MaNhanVien, nv.TenNhanVien, b.*
    	from
    	(
    		SELECT 
    			a.ID_NhanVien,
    			CAST(ROUND(SUM(a.HoaHongThucHien), 0) as float) as HoaHongThucHien,
    				CAST(ROUND(SUM(a.HoaHongThucHien_TheoYC), 0) as float) as HoaHongThucHien_TheoYC,
    			CAST(ROUND(SUM(a.HoaHongTuVan), 0) as float) as HoaHongTuVan,
    			CAST(ROUND(SUM(a.HoaHongBanGoiDV), 0) as float) as HoaHongBanGoiDV,   		
    			case @Status_ColumHide
    				when  1 then cast(0 as float)
    				when  2 then SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  3 then SUM(ISNULL(HoaHongBanGoiDV,0.0))
    				when  4 then SUM(ISNULL(HoaHongBanGoiDV,0.0)) + SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  5 then SUM(ISNULL(HoaHongTuVan,0.0))
    				when  6 then SUM(ISNULL(HoaHongThucHien_TheoYC,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0))
    				when  7 then SUM(ISNULL(HoaHongBanGoiDV,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0))
    					when  8 then SUM(ISNULL(HoaHongBanGoiDV,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0)) + SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  9 then SUM(ISNULL(HoaHongThucHien,0.0))
    				when  10 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  11 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongBanGoiDV,0.0)) 
    				when  12 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongBanGoiDV,0.0)) + SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  13 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0))
    					when  14 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0)) + SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    				when  15 then SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0)) + SUM(ISNULL(HoaHongBanGoiDV,0.0))
    			else SUM(ISNULL(HoaHongThucHien,0.0)) + SUM(ISNULL(HoaHongTuVan,0.0)) + SUM(ISNULL(HoaHongBanGoiDV,0.0))+  SUM(ISNULL(HoaHongThucHien_TheoYC,0.0))
    			end as Tong
    		FROM
    		(
    				select ckout.ID_NhanVien,
    					ckout.TrangThaiHD,
    					case when ckout.LoaiHoaDon= 6 then - HoaHongThucHien else HoaHongThucHien end as HoaHongThucHien,
    					case when ckout.LoaiHoaDon= 6 then - HoaHongTuVan else HoaHongTuVan end as HoaHongTuVan,
    					case when ckout.LoaiHoaDon= 6 then - HoaHongThucHien_TheoYC else HoaHongThucHien_TheoYC end as HoaHongThucHien_TheoYC,
    					case when ckout.LoaiHoaDon= 6 then - HoaHongBanGoiDV else HoaHongBanGoiDV end as HoaHongBanGoiDV				
    				from
    				(Select 
    						ck.ID_NhanVien,
    						hd.LoaiHoaDon,
    					Case when ck.ThucHien_TuVan = 1 and TheoYeuCau !=1 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongThucHien,
    					Case when ck.ThucHien_TuVan = 0 and (tinhchietkhautheo is null or tinhchietkhautheo!=4) then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongTuVan,
    					Case when ck.TheoYeuCau = 1 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongThucHien_TheoYC,
    					Case when ck.TinhChietKhauTheo = 4 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongBanGoiDV,
    						case when hd.ChoThanhToan='0' then 1 else 2 end as TrangThaiHD
    				from
    				BH_NhanVienThucHien ck
    				inner join BH_HoaDon_ChiTiet hdct on ck.ID_ChiTietHoaDon = hdct.ID
    				inner join BH_HoaDon hd on hd.ID = hdct.ID_HoaDon
    				Where hd.ChoThanhToan is not null
    					and hd.ID_DonVi in (select * from dbo.splitstring(@ID_ChiNhanhs))
						and (exists (select LoaiChungTu from @tblChungTu ctu where ctu.LoaiChungTu = hd.LoaiHoaDon))
    					and hd.NgayLapHoaDon >= @DateFrom 
    					and hd.NgayLapHoaDon < @DateTo   
    						and (exists (select ID from @tblNhanVien nv where ck.ID_NhanVien = nv.ID))
    					) ckout
    		) a where a.TrangThaiHD = @StatusInvoice
    		GROUP BY a.ID_NhanVien
    	) b
    		join NS_NhanVien nv on b.ID_NhanVien= nv.ID
    		where 
    			((select count(Name) from @tblSearchString b where     			
    				nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'				
    				)=@count or @count=0)		
    		),
    		count_cte
    		as (
    			select count(ID_NhanVien) as TotalRow,
    				CEILING(COUNT(ID_NhanVien) / CAST(@PageSize as float ))  as TotalPage,
    				sum(HoaHongThucHien) as TongHoaHongThucHien,
    				sum(HoaHongThucHien_TheoYC) as TongHoaHongThucHien_TheoYC,
    				sum(HoaHongTuVan) as TongHoaHongTuVan,
    				sum(HoaHongBanGoiDV) as TongHoaHongBanGoiDV,
    				sum(Tong) as TongAll
    			from data_cte
    			)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaNhanVien
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

            Sql(@"ALTER PROCEDURE [dbo].[SP_InsertChietKhauHoaDonTraHang_NhanVien]
    @ID_HoaDon [nvarchar](max),
    @TongTienTra [float],
    @ID_HoaDonTra [nvarchar](max),
    @ID_DonVi [nvarchar](max)
AS
BEGIN
set nocount on;
    declare @count_CKTraHang int
    	--- check xem co cai dat chiet khau TraHang khong
    	select @count_CKTraHang = count(hd.ID)
    	from ChietKhauMacDinh_HoaDon hd
    	where hd.ID_DonVi like @ID_DonVi
    	and hd.TrangThai !='0' and  hd.ChungTuApDung like '%6%'
    
    	if	@count_CKTraHang > 0
    		begin		
    			-- get PhaiThanhToan from HDMua --> chia % de tinh lai ChietKhau (theo VND)
    			declare @PhaiThanhToan float
    			select @PhaiThanhToan = TongThanhToan - isnull(TongTienThue,0) from BH_HoaDon where ID like @ID_HoaDon
    			
    			-- copy data from BH_NhanVienThucHien (HDMua) to BH_NhanVienThucHien (HDTra) with new {ID_HoaDon, TienChietKhau}
			insert into BH_NhanVienThucHien (ID, ID_NhanVien,  PT_ChietKhau, TinhChietKhauTheo, HeSo,ID_HoaDon,ThucHien_TuVan, TheoYeuCau, TienChietKhau)	
				
				select 
					NewID() as ID,
					ID_NhanVien,
					PT_ChietKhau,
					TinhChietKhauTheo,
					HeSo,
					@ID_HoaDonTra,
					ThucHien_TuVan,
					TheoYeuCau,
					case when TinhChietKhauTheo !=3 then (@TongTienTra * PT_ChietKhau *HeSo)/100
    				else (TienChietKhau/@PhaiThanhToan) * @TongTienTra end  as TienChietKhau
				from BH_NhanVienThucHien th
				where ID_HoaDon like @ID_HoaDon
				and TinhChietKhauTheo!=1 -- khong lay ck theothucthu

				--select 
				--	NewID() as ID,
				--	ID_NhanVien,
				--	PT_ChietKhau,
				--	TinhChietKhauTheo,
				--	HeSo,
				--	@ID_HoaDonTra,
				--	ThucHien_TuVan,
				--	TheoYeuCau,
				--	case when TinhChietKhauTheo !=3 then (@TongTienTra * PT_ChietKhau * HeSo)/100
    --				else (TienChietKhau/@PhaiThanhToan) * @TongTienTra end  as TienChietKhau
				--from
				--(
				--select 
				--	ID_NhanVien,PT_ChietKhau,TinhChietKhauTheo,HeSo,ThucHien_TuVan,TheoYeuCau,
				--	case when TinhChietKhauTheo !=3 then (@TongTienTra * PT_ChietKhau *HeSo)/100
    --				else (TienChietKhau/@PhaiThanhToan) * @TongTienTra end  as TienChietKhau
				--from BH_NhanVienThucHien th
				--where ID_HoaDon like @ID_HoaDon
				--and TinhChietKhauTheo!=1 -- khong lay ck theothucthu

				----union all
				----select 
				----	ID_NhanVien,PT_ChietKhau,TinhChietKhauTheo,HeSo,ThucHien_TuVan,TheoYeuCau,TienChietKhau
				----from(
				----select 
				----	th.*, ROW_NUMBER() over (partition by th.ID_NhanVien order by th.ID_NhanVien, TienChietKhau desc) as Rn
				----from BH_NhanVienThucHien th
				----where ID_HoaDon like @ID_HoaDon
				----and TinhChietKhauTheo =1 -- thucthu: neu thanhtoan nhieulan, lay ck lon nhat
				----) a where Rn= 1
				--) b
    		end
END");

        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[CTHD_GetDichVubyDinhLuong]");
			DropStoredProcedure("[dbo].[GetCTHDSuaChua_afterXuatKho]");
			DropStoredProcedure("[dbo].[HDSC_GetChiTietXuatKho]");
			DropStoredProcedure("[dbo].[InsertChietKhauTraHang_TheoThucThu]");
			DropStoredProcedure("[dbo].[UpdateIDCTNew_forCTOld]");
        }
    }
}
