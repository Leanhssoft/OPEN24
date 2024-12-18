﻿namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20211129 : DbMigration
    {
        public override void Up()
        {
			Sql(@"ALTER FUNCTION [dbo].[GetGiaVonOfDichVu]
(
	@ID_DonVi uniqueidentifier,
	@ID_DichVu uniqueidentifier	
)
RETURNS float
AS
BEGIN
	
	DECLARE @SumGiaVon float = 0

	declare @Level1_IDDichVu uniqueidentifier,@Level1_IDLoHang uniqueidentifier, @LaHangHoa bit, @MaHangHoa nvarchar(max), @SoLuong float
	declare _cur1 cursor for
    select 
		dl.ID_DonViQuiDoi,
		dl.ID_LoHang,
		hh.LaHangHoa,
		qd.MaHangHoa,
		dl.SoLuong
	from DinhLuongDichVu dl
	join DonViQuiDoi qd on dl.ID_DonViQuiDoi = qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	where dl.ID_DichVu= @ID_DichVu
	and dl.ID_DonViQuiDoi !=@ID_DichVu -- tránh trường hợp thành phần dịch vụ là chính nó
	
	open _cur1
	fetch next from _cur1 into @Level1_IDDichVu,@Level1_IDLoHang, @LaHangHoa, @MaHangHoa, @SoLuong
	while @@FETCH_STATUS =0
	begin
		if @LaHangHoa='0'
			begin		
				set @SumGiaVon += @SoLuong *  (select dbo.GetGiaVonOfDichVu(@ID_DonVi, @Level1_IDDichVu))				
			end
		else
			begin				
				set @sumGiaVon += @SoLuong 
					*  isnull((select top 1 GiaVon from DM_GiaVon 
						where ID_DonVi= @ID_DonVi and ID_DonViQuiDoi = @Level1_IDDichVu 
						and (ID_LoHang= @Level1_IDLoHang or (ID_LoHang is null and @Level1_IDLoHang is null))),0)	--- if tpdinhluong la lohang
			end	
		fetch next from _cur1 into @Level1_IDDichVu,@Level1_IDLoHang, @LaHangHoa,@MaHangHoa, @SoLuong
	end
	close _cur1
	deallocate _cur1
	RETURN @SumGiaVon

END");

			Sql(@"ALTER PROCEDURE [dbo].[DanhMucKhachHang_CongNo]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [uniqueidentifier],
    @MaKH [nvarchar](max),
    @LoaiKH [int],
    @ID_NhomKhachHang [nvarchar](max),
    @timeStartKH [datetime],
    @timeEndKH [datetime]
AS
BEGIN
	set nocount on

	declare @tblIDNhoms table (ID varchar(36))
	if @ID_NhomKhachHang ='%%'
		begin
			-- check QuanLyKHTheochiNhanh
			declare @QLTheoCN bit = (select QuanLyKhachHangTheoDonVi from HT_CauHinhPhanMem where ID_DonVi like @ID_ChiNhanh)
			insert into @tblIDNhoms(ID) values ('00000000-0000-0000-0000-000000000000')

			if @QLTheoCN = 1
				begin									
					insert into @tblIDNhoms(ID)
					select *  from (
						-- get Nhom not not exist in NhomDoiTuong_DonVi
						select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
						where not exists (select ID_NhomDoiTuong from NhomDoiTuong_DonVi where ID_NhomDoiTuong= nhom.ID) 
						and LoaiDoiTuong = @LoaiKH --and (TrangThai = 0)
						union all
						-- get Nhom at this ChiNhanh
						select convert(varchar(36),ID_NhomDoiTuong)  from NhomDoiTuong_DonVi where ID_DonVi like @ID_ChiNhanh) tbl
				end
			else
				begin				
				-- insert all
				insert into @tblIDNhoms(ID)
				select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
				where LoaiDoiTuong = @LoaiKH --and (TrangThai is null or TrangThai = 1)
				end		
		end
	else
		begin
			set @ID_NhomKhachHang = REPLACE(@ID_NhomKhachHang,'%','')
			insert into @tblIDNhoms(ID) values (@ID_NhomKhachHang)
		end

    SELECT  distinct *
    		FROM
    		(
    		  SELECT 
    		  dt.ID as ID,
    		  dt.MaDoiTuong, 
			  case when dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else  ISNULL(dt.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as ID_NhomDoiTuong,
    	      dt.TenDoiTuong,
    		  dt.TenDoiTuong_KhongDau,
    		  dt.TenDoiTuong_ChuCaiDau,
			  dt.ID_TrangThai,
    		  dt.GioiTinhNam,
    		  dt.NgaySinh_NgayTLap,
			  dt.NgayGiaoDichGanNhat,
    		  dt.DienThoai,
    		  dt.Email,
    		  dt.DiaChi,
    		  dt.MaSoThue,
    		  ISNULL(dt.GhiChu,'') as GhiChu,
    		  dt.NgayTao,
    		  dt.DinhDang_NgaySinh,
    		  ISNULL(dt.NguoiTao,'') as NguoiTao,
    		  dt.ID_NguonKhach,
    		  dt.ID_NhanVienPhuTrach,
    		  dt.ID_NguoiGioiThieu,
    		  dt.LaCaNhan,
    		  ISNULL(dt.TongTichDiem,0) as TongTichDiem,
			  case when right(rtrim(dt.TenNhomDoiTuongs),1) =',' then LEFT(Rtrim(dt.TenNhomDoiTuongs), len(dt.TenNhomDoiTuongs)-1) else ISNULL(dt.TenNhomDoiTuongs,N'Nhóm mặc định') end as TenNhomDT,-- remove last coma
    		  dt.ID_TinhThanh,
    		  dt.ID_QuanHuyen,
			  ISNULL(dt.TrangThai_TheGiaTri,1) as TrangThai_TheGiaTri,
			  ISNULL(trangthai.TenTrangThai,'') as TrangThaiKhachHang,
			  ISNULL(qh.TenQuanHuyen,'') as PhuongXa,
			  ISNULL(tt.TenTinhThanh,'') as KhuVuc,
			  ISNULL(dv.TenDonVi,'') as ConTy,
			  ISNULL(dv.SoDienThoai,'') as DienThoaiChiNhanh,
			  ISNULL(dv.DiaChi,'') as DiaChiChiNhanh,
			  ISNULL(nk.TenNguonKhach,'') as TenNguonKhach,
			  ISNULL(dt2.TenDoiTuong,'') as NguoiGioiThieu,
    	      CAST(ROUND(ISNULL(a.NoHienTai,0), 0) as float) as NoHienTai,
    		  CAST(ROUND(ISNULL(a.TongBan,0), 0) as float) as TongBan,
    		  CAST(ROUND(ISNULL(a.TongBanTruTraHang,0), 0) as float) as TongBanTruTraHang,
    		  CAST(ROUND(ISNULL(a.TongMua,0), 0) as float) as TongMua,
    		  CAST(ROUND(ISNULL(a.SoLanMuaHang,0), 0) as float) as SoLanMuaHang,
			  CAST(0 as float) as TongNapThe , 
			  CAST(0 as float) as SuDungThe , 
			  CAST(0 as float) as HoanTraTheGiaTri , 
			  CAST(0 as float) as SoDuTheGiaTri , 
			   a.PhiDichVu,
			  concat(dt.MaDoiTuong,' ',lower(dt.MaDoiTuong) ,' ', dt.TenDoiTuong,' ', dt.DienThoai,' ', dt.TenDoiTuong_KhongDau)  as Name_Phone
    		  FROM DM_DoiTuong dt    
				left join DM_DoiTuong_Nhom dtn on dt.ID= dtn.ID_DoiTuong
    			LEFT join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    			LEFT join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
				LEFT join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID
				LEFT join DM_DoiTuong dt2 on dt.ID_NguoiGioiThieu = dt2.ID --and dt2.LoaiDoiTuong= 1 and dt2.TheoDoi='0'
    			LEFT join DM_DonVi dv on dt.ID_DonVi = dv.ID
				LEFT join DM_DoiTuong_TrangThai trangthai on dt.ID_TrangThai = trangthai.ID
    			LEFT Join
    			(
    			  SELECT tblThuChi.ID_DoiTuong,
					SUM(ISNULL(tblThuChi.DoanhThu,0)) + SUM(ISNULL(tblThuChi.TienChi,0)) 
					- sum(isnull(tblThuChi.PhiDichVu,0)) 
					- SUM(ISNULL(tblThuChi.TienThu,0)) - SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS NoHienTai,
    				SUM(ISNULL(tblThuChi.DoanhThu, 0)) as TongBan,
    				SUM(ISNULL(tblThuChi.DoanhThu,0)) -  SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS TongBanTruTraHang,
    				SUM(ISNULL(tblThuChi.GiaTriTra, 0)) - SUM(ISNULL(tblThuChi.DoanhThu,0))  as TongMua,
    				SUM(ISNULL(tblThuChi.SoLanMuaHang, 0)) as SoLanMuaHang,
					sum(isnull(tblThuChi.PhiDichVu,0)) as PhiDichVu
    				FROM
    				(
						select 
							cp.ID_NhaCungCap as ID_DoiTuong,
							0 as GiaTriTra,
    						0 as DoanhThu,
							0 AS TienThu,
    						0 AS TienChi, 
    						0 AS SoLanMuaHang,				
							sum(cp.ThanhTien) as PhiDichVu
						from BH_HoaDon_ChiPhi cp
						join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
						where hd.ChoThanhToan = 0
						and hd.ID_DonVi= @ID_ChiNhanh
						group by cp.ID_NhaCungCap

						union all

							-- doanh thu
    						SELECT 
    							bhd.ID_DoiTuong,
    							0 AS GiaTriTra,
    							ISNULL(bhd.PhaiThanhToan,0) AS DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as PhiDichVu
    						FROM BH_HoaDon bhd
    						WHERE bhd.LoaiHoaDon in (1,7,19,22, 25) AND bhd.ChoThanhToan = 0 
							AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd
    						AND bhd.ID_DonVi = @ID_ChiNhanh

							
    						union all
							-- gia tri trả từ bán hàng
    						SELECT bhd.ID_DoiTuong,
    							ISNULL(bhd.PhaiThanhToan,0) AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								0 as PhiDichVu
    						FROM BH_HoaDon bhd   						
    						WHERE (bhd.LoaiHoaDon = '6' or bhd.LoaiHoaDon = 4) AND bhd.ChoThanhToan = 0  
							AND bhd.NgayLapHoaDon >= @timeStart AND bhd.NgayLapHoaDon < @timeEnd  						
    						AND bhd.ID_DonVi = @ID_ChiNhanh

							union all

							-- tienthu
							SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							ISNULL(qhdct.TienThu,0) AS TienThu,
    							0 AS TienChi,
								0 AS SoLanMuaHang,
								0 as PhiDichVu
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    						WHERE qhd.LoaiHoaDon = '11' AND  (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    						AND qhd.ID_DonVi = @ID_ChiNhanh
							AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd  
							
							union all

							-- tienchi
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    							ISNULL(qhdct.TienThu,0) AS TienChi,
								0 AS SoLanMuaHang,
								0 as PhiDichVu
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    						WHERE qhd.LoaiHoaDon = '12' AND (qhd.TrangThai != '0' OR qhd.TrangThai is null)
							AND qhd.NgayLapHoaDon >= @timeStart AND qhd.NgayLapHoaDon < @timeEnd  
    						AND qhd.ID_DonVi = @ID_ChiNhanh

							Union All
							-- solan mua hang
    						Select 
    							hd.ID_DoiTuong,
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
								0 as TienChi,
    							COUNT(*) AS SoLanMuaHang,
								0 as PhiDichVu
    						From BH_HoaDon hd 
    						where (hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 19)
    						and hd.ChoThanhToan = 0
    						AND hd.NgayLapHoaDon >= @timeStart AND hd.NgayLapHoaDon < @timeEnd 
							GROUP BY hd.ID_DoiTuong  						    					
							
    					)AS tblThuChi
    						GROUP BY tblThuChi.ID_DoiTuong
    				) a on dt.ID = a.ID_DoiTuong  					
						WHERE (dt.MaDoiTuong LIKE @MaKH 
							OR dt.TenDoiTuong_ChuCaiDau LIKE @MaKH  
							OR dt.TenDoiTuong_KhongDau LIKE @MaKH 
							OR dt.TenDoiTuong LIKE @MaKH 
    						OR dt.DienThoai LIKE @MaKH)
    					and dt.loaidoituong = @loaiKH
    					and dt.NgayTao >= @timeStartKH and dt.NgayTao < @timeEndKH
    					AND dt.TheoDoi =0
						and exists (select ID from @tblIDNhoms nhom where dtn.ID_NhomDoiTuong = nhom.ID OR dtn.ID_DoiTuong is null)	
						and dt.ID not like '%00000000-0000-0000-0000-0000%'
    				)b					
					--INNER JOIN @tblIDNhoms tblsearch ON CHARINDEX(CONCAT(', ', tblsearch.ID, ', '), CONCAT(', ', b.ID_NhomDoiTuong, ', '))>0
				order by b.ngaytao desc 
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetAllDinhLuongDichVu]
	@ID_DonVi nvarchar(max),
    @ID_DichVu nvarchar(max)
AS
BEGIN
	set nocount on;

	select a.*,
		a.SoLuong as SoLuongMacDinh,
		a.SoLuong as SoLuongDinhLuong_BanDau,
		a.SoLuong * a.GiaVon as ThanhTien,
		iif(a.LaHangHoa= '0',0, isnull(tk.TonKho,0)) as TonKho	
	from
	(
   select 
		dl.ID,
		dl.ID_DichVu,
		dl.ID_DonViQuiDoi,
		dl.ID_LoHang,
		dl.GhiChu,
		dl.SoLuong,
		dl.STT,
		iif(dl.DonGia is null, qd.GiaBan, dl.DonGia) as DonGia,
		qd.GiaBan,
		hh.LaHangHoa,
		hh.TenHangHoa,
		hh.TenHangHoa as TenHangHoaThayThe,
		isnull(hh.DonViTinhQuyCach,'') as DonViTinhQuyCach,
		iif(hh.QuyCach is null or hh.QuyCach < 1, 1, hh.QuyCach) as QuyCach,
		qd.MaHangHoa,
		qd.TenDonViTinh,
		iif(qd.TyLeChuyenDoi is null or qd.TyLeChuyenDoi = 0, 1,qd.TyLeChuyenDoi) as TyLeChuyenDoi,
		iif(hh.LaHangHoa ='0', dbo.GetGiaVonOfDichVu(@ID_DonVi, dl.ID_DonViQuiDoi),isnull(gv.GiaVon,0)) as GiaVon,
		iif(hh.QuyCach is null or hh.QuyCach =0, 1, hh.QuyCach) * dl.SoLuong as SoLuongQuyCach,
		iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
		lo.MaLoHang
	from DinhLuongDichVu dl
	join DonViQuiDoi qd on dl.ID_DonViQuiDoi = qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	left join DM_LoHang lo on dl.ID_LoHang = lo.ID 
	left join DM_GiaVon gv on dl.ID_DonViQuiDoi = gv.ID_DonViQuiDoi 
		and gv.ID_DonVi = @ID_DonVi	and (dl.ID_LoHang = gv.ID_LoHang or dl.ID_LoHang is null)
	where dl.ID_DichVu like @ID_DichVu
	and qd.Xoa='0'
	) a
	left join DM_HangHoa_TonKho tk on a.ID_DonViQuiDoi = tk.ID_DonViQuyDoi 
	and  tk.ID_DonVi = @ID_DonVi and (tk.ID_LoHang= a.ID_LoHang or a.ID_LoHang is null)
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetHoaDonDatHang_afterXuLy]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [uniqueidentifier],
    @txtSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    set nocount on;
	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@txtSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    SELECT 
    	c.ID,
    	c.MaHoaDon,
    	c.LoaiHoaDon,
    	c.NgayLapHoaDon,
    	c.TenDoiTuong,
    	c.DienThoai,
    	c.ID_NhanVien,
    	c.ID_DoiTuong,
    	c.ID_BangGia,
    	c.ID_DonVi,
    	c.YeuCau,
    		'' as MaHoaDonGoc,
    		ISNULL(c.MaDoiTuong,'') as MaDoiTuong,
    	ISNULL(c.NguoiTaoHD,'') as NguoiTaoHD,
    	c.TenNhanVien,
    	c.DienGiai,
    	c.TongTienHang, 
    		c.TongGiamGia, 
    		c.PhaiThanhToan, 
    		c.KhachDaTra,
    		c.TongChietKhau,
    		c.TongTienThue, 
			c.TongChiPhi, 
    	c.TrangThai,
    	c.HoaDon_HangHoa, -- string contail all MaHangHoa,TenHangHoa of HoaDon
		c.MaPhieuTiepNhan, c.BienSo, c.ID_PhieuTiepNhan, c.ID_Xe, c.ID_BaoHiem, 
		c.PTThueHoaDon,
		c.TenBaoHiem,
		c.ChoThanhToan,
		c.TongThanhToan
    	FROM
    	(
    		select 
    		a.ID as ID,
    		bhhd.MaHoaDon,
    		hdXMLOut.HoaDon_HangHoa,
    		bhhd.LoaiHoaDon,
    		bhhd.ID_NhanVien,
    		ISNULL(bhhd.ID_DoiTuong,'00000000-0000-0000-0000-000000000000') as ID_DoiTuong,
    		bhhd.ID_BangGia,
    		bhhd.NgayLapHoaDon,
    		bhhd.YeuCau,
    		bhhd.ID_DonVi,
    			dt.MaDoiTuong,
    			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
    		ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau,
    			ISNULL(dt.TenDoiTuong_ChuCaiDau, N'kl') as TenDoiTuong_ChuCaiDau,
    			ISNULL(dt.DienThoai, N'') as DienThoai,
    			ISNULL(nv.TenNhanVien, N'') as TenNhanVien,
    		bhhd.DienGiai,
    		bhhd.NguoiTao as NguoiTaoHD,
			isnull(bhhd.TongThanhToan, bhhd.PhaiThanhToan) as TongThanhToan,
    		CAST(ROUND(bhhd.TongTienHang, 0) as float) as TongTienHang,
    		CAST(ROUND(bhhd.TongGiamGia, 0) as float) as TongGiamGia,
    		CAST(ROUND(bhhd.PhaiThanhToan, 0) as float) as PhaiThanhToan,
    			CAST(ROUND(bhhd.TongTienThue, 0) as float) as TongTienThue,
				isnull(bhhd.TongChiPhi,0) as TongChiPhi,
				isnull(bhhd.PTThueHoaDon,0) as PTThueHoaDon,
    			a.KhachDaTra,
    			bhhd.TongChietKhau,
				bhhd.ID_BaoHiem,
    			bhhd.ChoThanhToan,
				bhhd.ID_PhieuTiepNhan,
				bhhd.ID_Xe,
				bh.TenDoiTuong as TenBaoHiem,	
				isnull(tn.MaPhieuTiepNhan,'') as MaPhieuTiepNhan,
				isnull(xe.BienSo,'') as BienSo,
    		Case When bhhd.YeuCau = '1' then N'Phiếu tạm' when bhhd.YeuCau = '3' then N'Hoàn thành' when bhhd.YeuCau = '2' then N'Đang giao hàng' else N'Đã hủy' end as TrangThai
    		FROM
    		(
    			select 
    			b.ID,
    			SUM(ISNULL(b.KhachDaTra, 0)) as KhachDaTra
    			from
    			(
    					-- get infor PhieuThu from HDDatHang (HuyPhieuThu (qhd.TrangThai ='0')
    				Select 
    					bhhd.ID,						
    					Case when qhd.TrangThai='0' then 0 else case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.Tienthu, 0) else -ISNULL(hdct.Tienthu, 0) end end as KhachDaTra,
    						0 as SoLuongBan,
    						0 as SoLuongTra				
    					from BH_HoaDon bhhd
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on hdct.ID_HoaDon = qhd.ID 				
    				where bhhd.LoaiHoaDon = '3' and bhhd.ChoThanhToan is not null
    					and bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd and bhhd.ID_DonVi = @ID_ChiNhanh  
    
    				union all
    					-- get infor PhieuThu/Chi from HDXuLy
    				Select
    					hdt.ID,						
    						Case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then (Case when qhd.LoaiHoaDon = 11 or qhd.TrangThai='0' then 0 else -ISNULL(hdct.Tienthu, 0) end)
    						else (Case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.Tienthu, 0) else -ISNULL(hdct.Tienthu, 0) end) end as KhachDaTra,
    						0 as SoLuongBan,
    						0 as SoLuongTra
    				from BH_HoaDon bhhd
    				inner join BH_HoaDon hdt on (bhhd.ID_HoaDon = hdt.ID and hdt.ChoThanhToan = '0')
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on (hdct.ID_HoaDon = qhd.ID)
    				where hdt.LoaiHoaDon = '3' 
    					and bhhd.ChoThanhToan='0'
    					and bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd and bhhd.ID_DonVi = @ID_ChiNhanh					
    			) b
    			group by b.ID 
    		) as a
    		inner join BH_HoaDon bhhd on a.ID = bhhd.ID
    		left join DM_DoiTuong dt on bhhd.ID_DoiTuong = dt.ID
    		left join NS_NhanVien nv on bhhd.ID_NhanVien = nv.ID 
			left join Gara_PhieuTiepNhan tn on bhhd.ID_PhieuTiepNhan= tn.ID
			left join Gara_DanhMucXe xe on tn.ID_Xe = xe.ID
			left join DM_DoiTuong bh on bhhd.ID_BaoHiem = bh.ID
    		left join 
    			(Select distinct hdXML.ID, 
    					(
    					select qd.MaHangHoa +', '  AS [text()], hh.TenHangHoa +', '  AS [text()]
    					from BH_HoaDon_ChiTiet ct
    					join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    					join DM_HangHoa hh on  hh.ID= qd.ID_HangHoa
    					where ct.ID_HoaDon = hdXML.ID
    					For XML PATH ('')
    				) HoaDon_HangHoa
    			from BH_HoaDon hdXML) hdXMLOut on a.ID= hdXMLOut.ID
    		) as c
    		where c.NgayLapHoadon >= @timeStart and c.NgayLapHoaDon < @timeEnd
    		and c.LoaiHoaDon = 3 and c.YeuCau in (1,2) and c.ChoThanhToan = 0
			and
				((select count(Name) from @tblSearchString b where     
				c.MaDoiTuong like '%'+b.Name+'%'
				or c.TenDoiTuong like '%'+b.Name+'%'
				or c.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or c.DienThoai like '%'+b.Name+'%'		
				or c.BienSo like '%'+b.Name+'%'
				or c.MaPhieuTiepNhan like '%'+b.Name+'%'						
				or c.MaHoaDon like '%'+b.Name+'%'	
				)=@count or @count=0)	
    	ORDER BY c.NgayLapHoaDon DESC
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetNhatKyBaoDuong_byCar]
    @ID_Xe [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    
    		 select hd.MaHoaDon, 				
    				 hd.NgayLapHoaDon,
    				 qd.MaHangHoa,
    				 qd.TenDonViTinh,
    				hh.TenHangHoa,
    				dt.MaDoiTuong,
    				dt.TenDoiTuong,
    				ct.SoLuong,
    				ct.GhiChu,
    				lich.LanBaoDuong,
    				lich.SoKmBaoDuong,
    				lich.TrangThai,
    				case lich.TrangThai
    					when 0 then N'Đã hủy'
    					when 1 then N'Chưa xử lý'
    					when 2 then N'Đã xử lý'
    					when 3 then N'Đã nhắc'
    					when 4 then N'Đã hủy'
    					when 5 then N'Quá hạn'
    				end as sTrangThai,
    				nv.NVThucHiens
    		 from BH_HoaDon_ChiTiet ct	
    		  join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
    		  join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan= tn.ID
    		  left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
    		  join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID 
    		  join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    		  join Gara_LichBaoDuong lich on ct.ID_LichBaoDuong= lich.ID
    		  left join 
    		  (
    			select distinct thout.ID_ChiTietHoaDon,
    			 (
    			  select distinct nv.TenNhanVien + ', ' as [text()]
    			  from BH_NhanVienThucHien th
    			  join NS_NhanVien nv on th.ID_NhanVien= nv.ID
    			  where th.ID_ChiTietHoaDon = thout.ID_ChiTietHoaDon
    			  for xml path('')
    			  ) NVThucHiens
    			  from BH_NhanVienThucHien thout
    		  ) nv on ct.ID = nv.ID_ChiTietHoaDon
    		  where (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    		  and hd.LoaiHoaDon= 25
    		  and hd.ChoThanhToan= 0
    		  and tn.ID_Xe= @ID_Xe
    		  and ct.ID_LichBaoDuong is not null
    		  order by hd.NgayLapHoaDon	desc, qd.MaHangHoa
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetNhatKySuDung_GDV]
    @IDChiNhanhs [nvarchar](max) = null,
    @IDCustomers [nvarchar](max) = null,
    @IDCars [nvarchar](max) = null,
    @CurrentPage [int] = null,
    @PageSize [int] = null
AS
BEGIN
    SET NOCOUNT ON;
    	declare @sql nvarchar(max) ='', @where nvarchar(max), @paramDefined nvarchar(max)
    	declare @tblDefined nvarchar(max)= N' declare @tblChiNhanh table(ID uniqueidentifier)
    								declare @tblCus table(ID uniqueidentifier)
    								declare @tblCar table(ID uniqueidentifier)'
    
    	set @where = N' where 1 = 1 and hd.ChoThanhToan =0 and ct.ChatLieu= 4 and ct.ID_ChiTietGoiDV is not null and (ct.ID_ChiTietDinhLuong= ct.id OR ct.ID_ChiTietDinhLuong IS NULL) '
    
    	if isnull(@CurrentPage,'') =''
    		set @CurrentPage = 0
    	if isnull(@PageSize,'') =''
    		set @PageSize = 20
    
    	if isnull(@IDChiNhanhs,'') !=''
    		begin
    			set @where = CONCAT(@where , ' and exists (select ID from @tblChiNhanh cn where ID_DonVi = cn.ID)')
    			set @sql = CONCAT(@sql, ' insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In) ;')
    		end
    	if isnull(@IDCustomers,'') !=''
    		begin
    			set @where = CONCAT(@where , ' and exists (select ID from @tblCus cus where hd.ID_DoiTuong = cus.ID)')
    			set @sql = CONCAT(@sql, ' insert into @tblCus select name from dbo.splitstring(@IDCustomers_In) ;')
    		end
    	
    	if isnull(@IDCars,'') !=''
    		begin
    			set @where = CONCAT(@where , ' and exists (select ID from @tblCar car where hd.ID_Xe = car.ID)')
    			set @sql = CONCAT(@sql, ' insert into @tblCar select name from dbo.splitstring(@IDCars_In) ;')
    		end
    	
    	set @sql = CONCAT(@tblDefined, @sql, N'
    		with data_cte
    as (
    SELECT hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon,
    	qd.MaHangHoa, hh.TenHangHoa, 
    		hh.TenHangHoa_KhongDau,
    		ct.SoLuong,
    	hdXMLOut.HDCT_NhanVien as NhanVienThucHien,
    	CT_ChietKhauNV.TongChietKhau
    	FROM BH_HoaDon_ChiTiet ct
    	join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.id
    	join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    	left join 
    			(Select distinct hdXML.ID,
    					(
    					select distinct (nv.TenNhanVien) +'', ''  AS [text()]
    					from BH_HoaDon_ChiTiet ct2
    					left join BH_NhanVienThucHien nvth on ct2.ID = nvth.ID_ChiTietHoaDon
    					left join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID
    					where ct2.ID = hdXML.ID 
    					For XML PATH ('''')
    				) HDCT_NhanVien
    			from BH_HoaDon_ChiTiet hdXML) hdXMLOut on ct.ID= hdXMLOut.ID
    	 left join 
    			(select ct3.ID, SUM(isnull(nvth2.TienChietKhau,0)) as TongChietKhau from BH_HoaDon_ChiTiet ct3
    			left join BH_NhanVienThucHien nvth2 on ct3.ID = nvth2.ID_ChiTietHoaDon
    			group by ct3.ID) CT_ChietKhauNV on CT_ChietKhauNV.ID = ct.ID        
    	', @where, 
    		'),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize_In as float ))  as TotalPage,
    				sum(SoLuong) as TongSoLuong,
    				sum(TongChietKhau) as TongHoaHong			
    			from data_cte
    		)
    	select dt.*,
    		cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.NgayLapHoaDon desc
    		OFFSET (@CurrentPage_In * @PageSize_In) ROWS
    		FETCH NEXT @PageSize_In ROWS ONLY ')
    
    		print @sql
    
    		set @paramDefined =N'
    			@IDChiNhanhs_In nvarchar(max),
    			@IDCustomers_In nvarchar(max),
    			@IDCars_In nvarchar(max),
    			@CurrentPage_In int,
    			@PageSize_In int'
    
    		exec sp_executesql @sql, 
    		@paramDefined,
    		@IDChiNhanhs_In = @IDChiNhanhs,
    		@IDCustomers_In = @IDCustomers,
    		@IDCars_In = @IDCars,
    		@CurrentPage_In = @CurrentPage,
    		@PageSize_In = @PageSize
END");

			Sql(@"ALTER PROCEDURE [dbo].[HDSC_GetChiTietXuatKho]
    @ID_HoaDon [uniqueidentifier],
    @IDChiTietHD [uniqueidentifier],
    @LoaiHang [int]
AS
BEGIN
    SET NOCOUNT ON;
	-- get loaihoadon
	declare @LoaiHoaDon int = (select top 1 LoaiHoaDon from BH_HoaDon where ID= @ID_HoaDon)
	if @LoaiHoaDon =1 or @LoaiHoaDon = 6
	begin
		if	@LoaiHang = 1 -- hanghoa
		begin
			select 
    			qd.MaHangHoa, qd.TenDonViTinh,
    			hh.TenHangHoa,
				iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe,
    			lo.MaLoHang,
    			ct.SoLuong,
    			ct.SoLuong* round(ct.GiaVon ,3) as GiaVon, ---- giatrixuat
    			hd.MaHoaDon,
    			hd.NgayLapHoaDon,
    			ct.GhiChu,
				ct.ChatLieu
    		from BH_HoaDon_ChiTiet ct
			join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
			join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    		left join DM_LoHang lo on ct.ID_LoHang= lo.ID
			WHERE ct.id= @IDChiTietHD
		end
		else
		begin
			select 
    			qd.MaHangHoa, qd.TenDonViTinh,
    			hh.TenHangHoa,
				iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe,
    			lo.MaLoHang,
    			ct.SoLuong,
    			ct.SoLuong *  round(ct.GiaVon ,3) as GiaVon,
    			hd.MaHoaDon,
    			hd.NgayLapHoaDon,
    			ct.GhiChu,
				ct.ChatLieu
    		from BH_HoaDon_ChiTiet ct
			join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
			join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    		left join DM_LoHang lo on ct.ID_LoHang= lo.ID
			WHERE ct.ID_ChiTietDinhLuong= @IDChiTietHD and ct.id!=@IDChiTietHD
		end
	end
	else
		begin	   
    		if	@LoaiHang = 1 -- hanghoa
    		begin
    		select 
    			qd.MaHangHoa, qd.TenDonViTinh,
    			hh.TenHangHoa,
				iif(pxk.TenHangHoaThayThe is null or pxk.TenHangHoaThayThe ='', hh.TenHangHoa, pxk.TenHangHoaThayThe) as TenHangHoaThayThe,
    			lo.MaLoHang,
    			pxk.SoLuong,
    			round(pxk.GiaVon ,3) as GiaVon,
    			pxk.MaHoaDon,
    			pxk.NgayLapHoaDon,
    			pxk.GhiChu,
				pxk.ChatLieu
    		from(
    			select 
    				hd.MaHoaDon,
    				hd.NgayLapHoaDon,
    				ctxk.ID_DonViQuiDoi,
    				ctxk.ID_LoHang,
    				ctxk.SoLuong,
    				ctxk.SoLuong * ctxk.GiaVon as GiaVon,
    				ctxk.GhiChu,
					ctxk.TenHangHoaThayThe,
					ctxk.ChatLieu
    			from BH_HoaDon_ChiTiet ctxk
    			join BH_HoaDon hd on ctxk.ID_HoaDon= hd.ID
    			where (ctxk.ID_ChiTietGoiDV = @IDChiTietHD	
					or (ctxk.ID= @IDChiTietHD and ctxk.ChatLieu='1'))
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
					hh.TenHangHoa as TenHangHoaThayThe,					
    				qd.MaHangHoa, qd.TenDonViTinh, qd.ThuocTinhGiaTri,
    				isnull(lo.MaLoHang,'') as MaLoHang,
    				tpdl.SoLuongDinhLuong_BanDau,
    				round(tpdl.GiaTriDinhLuong_BanDau,3) as GiaTriDinhLuong_BanDau ,
    				tpdl.MaHoaDon,
    				tpdl.NgayLapHoaDon	,
    				tpdl.SoLuongXuat as SoLuong,
    				round(tpdl.GiaTriXuat,3) as GiaVon,
    				tpdl.GhiChu,
    				tpdl.LaDinhLuongBoSung,
					tpdl.ChatLieu
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
    							0 as LaDinhLuongBoSung,
								ct.ChatLieu
    						from BH_HoaDon_ChiTiet ct
    						left join
    						(
    							---- get tpdl when xuatkho (ID_ChiTietGoiDV la hanghoa)
    							select 
    				
    									hd.MaHoaDon,
    									hd.NgayLapHoaDon,
    									ct.SoLuong as SoLuongXuat,
    									round(ct.SoLuong * ct.GiaVon,3) as GiaTriXuat,
    									ct.GhiChu,
    									ct.ID_ChiTietGoiDV
    							from BH_HoaDon_ChiTiet ct
    							join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
    							where hd.ChoThanhToan='0' and ct.ID_ChiTietGoiDV is not null
								and hd.LoaiHoaDon in (1,6,8) --- sudung khi tra combo hdle
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
    							1 as LaDinhLuongBoSung,
								ct.ChatLieu
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
    		
    	end
		end
END");

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetQuyHoaDon_ofDoiTuong]
    @ID_DoiTuong [nvarchar](max),
    @ID_DonVi [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;
	if @ID_DonVi='00000000-0000-0000-0000-000000000000'
		begin
			set @ID_DonVi = (select CAST(ID as varchar(40)) + ',' as  [text()] from DM_DonVi  where TrangThai is null or TrangThai='1' for xml path(''))	
			set @ID_DonVi= left(@ID_DonVi, LEN(@ID_DonVi) -1) -- remove last comma ,
		end

	declare @LoaiDoiTuong int = (select LoaiDoiTuong from DM_DoiTuong where ID= @ID_DoiTuong)

		select tbl.ID_HoaDonLienQuan, 
			sum(tbl.TongTienThu + tbl.ThuDatHang) as TongTienThu
		from
		(
		
		select ID_HoaDonLienQuan,
			sum(TongTienThu) as TongTienThu,
			0 as ThuDatHang

		from
		(		-- thutien hoadon
				select ct.ID_HoaDonLienQuan,
    				case when hd.LoaiHoaDon = 6 or hd.LoaiHoaDon = 4 then sum(ISNULL(ct.TienThu,0)) else 
    				case when qhd.LoaiHoaDon = 11 or @LoaiDoiTuong = 2 then sum(ct.TienThu) else sum(ISNULL(-ct.TienThu,0)) end end TongTienThu    			
    			from Quy_HoaDon_ChiTiet ct
    			join Quy_HoaDon qhd on ct.ID_HoaDon = qhd.ID
    			left join BH_HoaDon hd on ct.ID_HoaDonLienQuan = hd.ID
    			where ct.ID_DoiTuong like @ID_DoiTuong 
				--and exists (select Name from dbo.splitstring(@ID_DonVi) where Name= qhd.ID_DonVi)
    			and (TrangThai is  null or TrangThai = '1' ) 
				and hd.LoaiHoaDon !=3
    			group by ct.ID_HoaDonLienQuan, hd.LoaiHoaDon,TrangThai, qhd.LoaiHoaDon
		) quy group by ID_HoaDonLienQuan
		
		union all

			select thuDH.ID_HoaDonMua, 
				0 as TongTienThu,
				thuDH.ThuDatHang
			from
			(
			-- neu hd xuly tu dathang --> lay phieuthu dathang
			select tblDH.ID_HoaDonMua,
					sum(tblDH.TienThu) as ThuDatHang,		
					ROW_NUMBER() OVER(PARTITION BY tblDH.ID ORDER BY tblDH.NgayLapHoaDon ASC) AS isFirst	--- group by hdDat, sort by ngaylap hdxuly
				from
				(			
						select hd.ID as ID_HoaDonMua, hd.NgayLapHoaDon,		
							hdd.ID,
							iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu			
						from Quy_HoaDon_ChiTiet qct
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
						join BH_HoaDon hdd on hdd.ID= qct.ID_HoaDonLienQuan
						join BH_HoaDon hd on hd.ID_HoaDon= hdd.ID
						where hdd.LoaiHoaDon = 3 	
						and hd.ChoThanhToan = 0 and hdd.ChoThanhToan='0' 
						and (qhd.TrangThai= 1 Or qhd.TrangThai is null)
						and hd.ID_DoiTuong like @ID_DoiTuong 
						) tblDH group by tblDH.ID_HoaDonMua, tblDH.ID,tblDH.NgayLapHoaDon
				) thuDH where thuDH.isFirst= 1
		)tbl group by tbl.ID_HoaDonLienQuan  
 
END");

			Sql(@"ALTER PROCEDURE [dbo].[TinhLaiBangLuong]
    @ID_BangLuong [uniqueidentifier],
    @NguoiSua [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	declare @IDNhanVienLogin uniqueidentifier= (select top 1 ID_NhanVien from HT_NguoiDung where LaAdmin='1')
    
    	select bl.TuNgay, bl.DenNgay, bl.ID_DonVi, ct.ID_NhanVien	
    	into #tempbangluong
    	from NS_BangLuong_ChiTiet ct
    	join NS_BangLuong bl on ct.ID_BangLuong= bl.ID
    	where bl.ID like @ID_BangLuong
    
    	declare @IDChiNhanhs uniqueidentifier, @FromDate datetime, @ToDate datetime, @KieuLuongs varchar(10)= '1,2,3,4'
    	select @IDChiNhanhs = ID_DonVi, @FromDate = TuNgay, @ToDate = DenNgay from (select top 1 * from  #tempbangluong ) a
    
    
    	declare @IDNhanViens varchar(max) = 	
    		(select cast(ID_NhanVien as varchar(40)) +',' AS [text()]
    		from #tempbangluong
    		for xml path('')
    		)   
    	
    
    		set @IDNhanViens = LEFT(@IDNhanViens,LEN(@IDNhanViens)-1) -- remove last comma
    
    		declare @ngaycongchuan float = (select dbo.TinhNgayCongChuan(@FromDate,@ToDate,@IDChiNhanhs))
    		
    		declare @tblCong CongThuCong
    		insert into @tblCong
    		exec dbo.GetChiTietCongThuCong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    		declare @tblThietLapLuong ThietLapLuong
    		insert into @tblThietLapLuong
    		exec GetNS_ThietLapLuong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    		declare @tblLuong table (LoaiLuong int, ID_NhanVien uniqueidentifier, LuongCoBan float, SoNgayDiLam float, LuongChinh float)				
    		insert into @tblLuong		
    		exec TinhLuongCoBan @ngaycongchuan, @tblCong, @tblThietLapLuong
		
    
    		declare @tblLuongOT table (ID_NhanVien uniqueidentifier, LuongOT float)				
    		insert into @tblLuongOT		
    		exec TinhLuongOT @ngaycongchuan, @tblCong, @tblThietLapLuong

			
    		declare @tblPhuCap table (ID_NhanVien uniqueidentifier, PhuCapCoDinh float, PhuCapTheoNgayCong float)
    		insert into @tblPhuCap
    		exec TinhPhuCapLuong @tblCong, @tblThietLapLuong
			
    
    		declare @tblPhuCapTheoPtram table (ID_NhanVien uniqueidentifier,ID_PhuCap uniqueidentifier, TenPhuCap nvarchar(max),  PhuCapCoDinh float, HeSo float, NgayApDung datetime, NgayKetThuc datetime, SoNgayDiLam float)
    		insert into @tblPhuCapTheoPtram
    		exec GetPhuCapCoDinh_TheoPtram @IDChiNhanhs, @FromDate, @ToDate, '%%', @tblCong, @tblThietLapLuong
    
    		declare @tblGiamTru table (ID_NhanVien uniqueidentifier, GiamTruCoDinhVND float, GiamTruTheoLan float, SoLanDiMuon float)
    		insert into @tblGiamTru
    		exec TinhGiamTruLuong @tblCong, @tblThietLapLuong	

				
    
    	----	 get phucap codinh theo %luongchinh
    	declare @tblLuongPC table (ID_NhanVien uniqueidentifier,LoaiLuong int, LuongCoBan float, SoNgayDiLam float, LuongChinh float,PhuCapCoDinh_TheoPtramLuong float)						
    	insert into @tblLuongPC	
    	select 
    		pcluong.ID_NhanVien, pcluong.LoaiLuong, pcluong.LuongCoBan, pcluong.SoNgayDiLam, pcluong.LuongChinh, 
    		sum(PhuCapCoDinh_TheoPtramLuong) as PhuCapCoDinh_TheoPtramLuong
    	from
    		(select luong.ID_NhanVien, LoaiLuong, LuongCoBan, luong.SoNgayDiLam, LuongChinh,
    			case when PhuCapCoDinh is null then 0 else LuongChinh * PhuCapCoDinh * HeSo/100 end as PhuCapCoDinh_TheoPtramLuong
    		from @tblLuong luong
    		left join @tblPhuCapTheoPtram pc on luong.ID_NhanVien= pc.ID_NhanVien
    		) pcluong 
    		group by pcluong.ID_NhanVien, pcluong.LuongChinh, pcluong.LoaiLuong, pcluong.LuongCoBan,pcluong.SoNgayDiLam
    

			---- ===== Tinhluong trong khoangthoigian

			declare @tblFromTo table (ID_BangLuong uniqueidentifier null, DateFrom datetime, DateTo datetime, isGiaoNhau int)

			select bl.ID, bl.MaBangLuong, bl.ID_DonVi, 
			 bl.TuNgay, bl.DenNgay
			into #tblTemp
			from NS_BangLuong bl
			where bl.TrangThai in (3,4)
			and ID_DonVi = @IDChiNhanhs
			and (
				(@FromDate <= TuNgay and @ToDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate >= TuNgay and @FromDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate >= TuNgay and @ToDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate <= TuNgay and  @ToDate >= DenNgay)
				)
	

			----====  get cac khoang thoi gian ----
				declare @cur_IDBangLuong uniqueidentifier, @cur_FDate datetime, @cur_TDate datetime
				declare _cur cursor
				for
					select ID, TuNgay, DenNgay
					from #tblTemp
				open _cur
				fetch next from _cur
				into @cur_IDBangLuong, @cur_FDate, @cur_TDate
				while @@FETCH_STATUS = 0
				begin
					
					if @FromDate < @cur_FDate
					begin
						if @ToDate < @cur_FDate
							insert into @tblFromTo values (null, @FromDate, @ToDate,0)
						else
							if @ToDate >= @cur_FDate and @ToDate < @cur_TDate
								insert into @tblFromTo values (null, @FromDate, @cur_FDate,0),
								(@cur_IDBangLuong, @cur_FDate, @ToDate,1)
							else
								insert into @tblFromTo values (null, @FromDate, @cur_FDate,0),
								(@cur_IDBangLuong, @cur_FDate, @cur_TDate,1),
								(null, @cur_TDate, @ToDate,0)								
					end
					else
					begin
						if @ToDate < @cur_TDate
							insert into @tblFromTo values (@cur_IDBangLuong, @FromDate, @ToDate,1)
						else
						insert into @tblFromTo values (@cur_IDBangLuong, @FromDate, @cur_TDate,1),
							(null, @cur_TDate, @ToDate,0)
					end
					fetch next from _cur into @cur_IDBangLuong, @cur_FDate, @cur_TDate
				end
				close _cur
				deallocate _cur

				drop table #tblTemp

				if (select count(*) from @tblFromTo) = 0
					insert into @tblFromTo values (null, @FromDate, @ToDate,0)

				------ ====== lay ds nhan vien thuoc (khong thuoc bang luong)
				
				declare @tblNhanVien table (ID_NhanVien uniqueidentifier)
				DECLARE @tab_HangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, 
				HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    			TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, 
				TongHoaHongBanGoiDV float, TongAll float)			

				DECLARE @tab_HoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    			TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)		

				DECLARE @tab_DoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    			Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float)     			

				---- temp
				DECLARE @temp_CKHangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, 
				HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    			TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, 
				TongHoaHongBanGoiDV float, TongAll float)

				DECLARE @temp_CKHoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    			TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)			

				DECLARE @temp_CKDoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    			Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float)     			   		

				declare @cur2_IDBangLuong uniqueidentifier, @cur2_FDate datetime, @cur2_TDate datetime, @cur2_isGiaoNhau int
				declare _cur2 cursor for
					select distinct * from @tblFromTo where DateFrom != DateTo	order by DateFrom
				open _cur2
				fetch next from _cur2
				into @cur2_IDBangLuong, @cur2_FDate, @cur2_TDate, @cur2_isGiaoNhau			
				while @@FETCH_STATUS =0
				begin			
					
					---- get nv exist bangluong				
					insert into @tblNhanVien
					select ct.ID_NhanVien
					from NS_BangLuong_ChiTiet ct
					join NS_BangLuong bl on ct.ID_BangLuong= bl.ID
					join NS_NhanVien nv on ct.ID_NhanVien= nv.ID
					where ct.ID_BangLuong = @cur2_IDBangLuong 					

					---- get ck hanghoa from-to
					insert into @temp_CKHangHoa
					exec ReportDiscountProduct_General @IDChiNhanhs, @IDNhanVienLogin,'%%','0,1,6,19,22,25', @cur2_FDate,@cur2_TDate, 16,1,0,100

					---- get ck hoadon from-to
					insert into @temp_CKHoaDon
					exec ReportDiscountInvoice @IDChiNhanhs,@IDNhanVienLogin,'%%','0,1,6,19,22,25', @cur2_FDate, @cur2_TDate, 8,1,0,0,100

					---- get ck doanhthu from-to
					insert into @temp_CKDoanhSo
					exec GetAll_DiscountSale @IDChiNhanhs,@IDNhanVienLogin, @cur2_FDate, @cur2_TDate, '%%', '', 0,1000		
				
					insert into @tab_HangHoa
					select * 
					from @temp_CKHangHoa ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)

					insert into @tab_HoaDon
					select * 
					from @temp_CKHoaDon ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)

					insert into @tab_DoanhSo
					select * 
					from @temp_CKDoanhSo ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)						

					---- neu da tao bangluong, update phucap codinh = 0
					if @cur2_IDBangLuong is not null
					begin
						update pc set pc.PhuCapCoDinh =0
						from @tblPhuCap pc
						where exists (
							select nv.ID_NhanVien
							from @tblNhanVien nv where pc.ID_NhanVien = nv.ID_NhanVien
						)

						update pc set pc.GiamTruCoDinhVND =0
						from @tblGiamTru pc
						where exists (
							select nv.ID_NhanVien
							from @tblNhanVien nv where pc.ID_NhanVien = nv.ID_NhanVien
						)
					end

					delete from @temp_CKHangHoa
					delete from @temp_CKHoaDon
					delete from @temp_CKDoanhSo
					delete from @tblNhanVien

					fetch next from _cur2
					into @cur2_IDBangLuong, @cur2_FDate, @cur2_TDate, @cur2_isGiaoNhau
				end
				close _cur2
				deallocate _cur2
    
    
    	----- hoahong		
    	--DECLARE @tab_DoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    	--Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float) 
    	--INSERT INTO @tab_DoanhSo exec GetAll_DiscountSale @IDChiNhanhs,@IDNhanVienLogin, @FromDate, @ToDate, '%%', '', 0,1000	
    
    	--DECLARE @tab_HoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    	--TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)
    	--INSERT INTO @tab_HoaDon exec ReportDiscountInvoice @IDChiNhanhs,@IDNhanVienLogin,'%%','0,1,6,19,22,25', @FromDate, @ToDate, 8,1,0,0,100
    
    	--DECLARE @tab_HangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    	--TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, TongHoaHongBanGoiDV float, TongAll float)
    	--INSERT INTO @tab_HangHoa exec ReportDiscountProduct_General @IDChiNhanhs,@IDNhanVienLogin,'%%', '0,1,6,19,22,25',@FromDate, @ToDate, 16,1,0,100

    
    	declare @tblHoaHong table (ID_NhanVien uniqueidentifier, TongDoanhThu float, HoaHong float, HoaHongHangHoa float, HoaHongHoaDon float, HoaHongDoanhThu float)
    	insert into  @tblHoaHong
    	SELECT a.ID_NhanVien, sum(TongDoanhThu) as TongDoanhThu,				
    		SUM(TongDoanhSo + TongHoaDon + TongHangHoa) as HoaHong,
    		SUM(TongHangHoa) as HoaHongHangHoa,
    		SUM(TongHoaDon) as HoaHongHoaDon,
    		SUM(TongDoanhSo) as HoaHongDoanhThu
    	FROM 
    	(
    		select ID_NhanVien,	Tong as TongHangHoa,0 as TongHoaDon, 0 as TongDoanhSo, 0 as TongDoanhThu						
    		from @tab_HangHoa
    		UNION ALL
    		Select ID_NhanVien,	0 as TongHangHoa,TongAll as TongHoaDon,	0 as TongDoanhSo, 0 as TongDoanhThu							
    		from @tab_HoaDon
    		UNION ALL
    		Select ID_NhanVien,	0 as TongHangHoa,0 as TongHoaDon,TongAll as TongDoanhSo, TongDoanhThu
    		from @tab_DoanhSo
    	) as a
    	GROUP BY a.ID_NhanVien
    
    	-- giamtru codinh %tongluongnhan
    	declare @tblGiamTruTheoPTram table (ID_NhanVien uniqueidentifier,ID_PhuCap uniqueidentifier, TenPhuCap nvarchar(max),  GiamTruCoDinh float, HeSo float,
    		NgayApDung datetime, NgayKetThuc datetime, SoLanDiMuon float)
    	insert into @tblGiamTruTheoPTram
    	exec GetGiamTruCoDinh_TheoPtram @IDChiNhanhs, @FromDate, @ToDate, '%%',@tblCong, @tblThietLapLuong	
    
    	select nv.MaNhanVien, nv.TenNhanVien, 
    			luong.*,
    			cast(PhuCapCoBan + PhuCapKhac + PhuCapCoDinh_TheoPtramLuong as float) as PhuCap,
    			cast(PhatDiMuon + GiamTruCoDinhVND as float) as TongTienPhat,
    			cast(LuongChinh as float)  as TongLuongNhan, -- save to DB
    			cast(LuongChinh + LuongOT +  PhuCapCoBan + PhuCapKhac + PhuCapCoDinh_TheoPtramLuong + KhenThuong + ChietKhau - PhatDiMuon - GiamTruCoDinhVND as float) as LuongThucNhan
    	into #tblluong
    		from
    			(
    			select 
    				tbl.ID_NhanVien, 
    				max(tbl.LoaiLuong) as LoaiLuong,
    				max(tbl.LuongCoBan) as LuongCoBan,
    				sum(tbl.LuongChinh) as LuongChinh,
    				sum(LuongOT) as LuongOT,	
    				sum(PhuCapCoDinh_TheoPtramLuong) as PhuCapCoDinh_TheoPtramLuong,				
    				sum(PhuCapCoDinh) as PhuCapCoBan,
    				sum(PhuCapTheoNgay) as PhuCapKhac,
    				sum(GiamTruCoDinhVND) as GiamTruCoDinhVND,
    				sum(GiamTruTheoLan) as PhatDiMuon,
    				sum(HoaHong) as ChietKhau,
    				sum(HoaHongHangHoa) as HoaHongHangHoa,
    				sum(HoaHongHoaDon) as HoaHongHoaDon,
    				sum(HoaHongDoanhThu) as HoaHongDoanhThu,
    				sum(TongDoanhThu) as TongDoanhThu,
    				sum(SoNgayDiLam) as NgayCongThuc,
    				sum(SoGioOT) as SoGioOT,
    				sum(SoLanDiMuon) as SoLanDiMuon,
    				sum(KhenThuong) as KhenThuong,
    				@ngaycongchuan as  NgayCongChuan
    			from 
    				(select 
    					ID_NhanVien, LoaiLuong, LuongCoBan, LuongChinh,
    					cast(0 as float) as LuongOT, 
    					SoNgayDiLam, cast(0 as float) as SoGioOT, 
    					PhuCapCoDinh_TheoPtramLuong,
    					cast(0 as float) as PhuCapCoDinh, cast(0 as float) as PhuCapTheoNgay,
    					cast(0 as float) as GiamTruCoDinhVND, cast(0 as float) as GiamTruTheoLan, cast(0 as float) as SoLanDiMuon,
    					cast(0 as float) as HoaHong,cast(0 as float) as HoaHongHangHoa, cast(0 as float) as HoaHongHoaDon, cast(0 as float) as HoaHongDoanhThu, 0 as TongDoanhThu,
    					cast(0 as float) as KhenThuong
    				from @tblLuongPC
    
    				union all
    
    				select 
    					ID_NhanVien, 0 as LoaiLuong, 0 as LuongCoBan, 0 as LuongChinh,
    					LuongOT,
    					0 as SoNgayDiLam,0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgay,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					0 as HoaHong, 0 as HoaHongHangHoa,0 as HoaHongHoaDon, 0 as HoaHongDoanhThu, 0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblLuongOT
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong,0 as LuongCoBan, 0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					PhuCapCoDinh, PhuCapTheoNgayCong,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					0 as HoaHong,0 as HoaHongHangHoa, 0 as HoaHongHoaDon, 0 as HoaHongDoanhThu,0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblPhuCap
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong,0 as LuongCoBan,0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgayCong,
    					GiamTruCoDinhVND, GiamTruTheoLan, SoLanDiMuon,
    					0 as HoaHong,0 as HoaHongHangHoa, 0 as HoaHongHoaDon, 0 as HoaHongDoanhThu,0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblGiamTru
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong, 0 as LuongCoBan,0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT, 
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgayCong,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					HoaHong, HoaHongHangHoa, HoaHongHoaDon, HoaHongDoanhThu, TongDoanhThu,
    					0 as KhenThuong
    				from @tblHoaHong
    				) tbl group by tbl.ID_NhanVien
    			) luong
    		join NS_NhanVien nv on luong.ID_NhanVien= nv.ID
		
    
    		-- get max maphieuluong 
    		declare @maxcodePL varchar(20) = (select ISNULL(MAX(CAST (dbo.udf_GetNumeric(MaBangLuongChiTiet) AS float)),0) 
    		from NS_BangLuong_ChiTiet where ID_BangLuong != @ID_BangLuong)

			---- if bangluong da thanhtoan --> get chi tiet phieuluong (ID_QuyCT, ID_bangluongCT, ID_NhanVien)
			select qct.ID, qct.ID_BangLuongChiTiet, qct.ID_NhanVien
			into #qctLuong
			from Quy_HoaDon_ChiTiet qct
			join NS_BangLuong_ChiTiet blct on qct.ID_BangLuongChiTiet = blct.ID
			where blct.ID_BangLuong = @ID_BangLuong
    
    		exec DeleteBangLuongChiTietById @ID_BangLuong
    		
    		insert into NS_BangLuong_ChiTiet
    		select  
    			NEWID(), @ID_BangLuong, ID_NhanVien, NgayCongThuc, NgayCongChuan, LuongCoBan, 
    			PhuCapCoBan,
    			PhuCapKhac + isnull(PhuCapCoDinh_TheoPtramLuong,0) as PhuCapKhac,
    			KhenThuong,
    			0, 0, 0,0, -- thue, mienthue, baohiem,kyluat
    			PhatDiMuon,
    			LuongOT,
    			LuongThucNhan - GiamTruCoDinh_TheoPTram, -- luong chinh + ot + hoahong + phucap - phat
    			TongTienPhat + GiamTruCoDinh_TheoPTram,
    			TongLuongNhan, -- luongchinh
    			N'tính lại lương', -- ghichu
    			1, -- tranngthai
    			@NguoiSua, GETDATE(),
    			@NguoiSua, GETDATE(),
    			0, -- baohiemcty
    			TongDoanhThu, -- doanhso
    			ChietKhau,
    			MaPhieu
    		from
    			(
    				select *, CONCAT('PL0000', RN + @maxcodePL) as MaPhieu
    				from
    				(
    					select luong.* ,
    						ISNULL(ISNULL(gt.GiamTruCoDinh,0) * gt.HeSo * luong.TongLuongNhan/100,0) as GiamTruCoDinh_TheoPTram,
    						ROW_NUMBER() over (order by luong.MaNhanVien) RN 
    					from #tblluong luong
    					left join @tblGiamTruTheoPTram gt on luong.ID_NhanVien= gt.ID_NhanVien
    					where exists(select Name from dbo.splitstring(@KieuLuongs) kl where luong.LoaiLuong= kl.Name)
    					and exists(select Name from dbo.splitstring(@IDNhanViens) nv where luong.ID_NhanVien= nv.Name)
						and luong.LuongThucNhan !=0
    				) pluong
    			) a
    
    		update NS_BangLuong set TrangThai= 1, NguoiSua = @NguoiSua, NgaySua= GETDATE() where id= @ID_BangLuong
    
    		---- update status, id_bangluongchitiet in NS_CongBoSung
    		exec UpdateStatusCongBoSung_WhenCreatBangLuong @ID_BangLuong, @FromDate, @ToDate

			---- update again quyct with new idbangluongct
			update qct1 set qct1.ID_BangLuongChiTiet= a.ID_BangLuongChiTiet
			from Quy_HoaDon_ChiTiet qct1
			join (
			select qct.ID, blct.ID as ID_BangLuongChiTiet
			from NS_BangLuong_ChiTiet blct
			join #qctLuong qct on blct.ID_NhanVien = qct.ID_NhanVien
			where blct.ID_BangLuong = @ID_BangLuong
			) a on qct1.ID = a.ID
END");

			Sql(@"ALTER PROCEDURE [dbo].[TinhLuongNhanVien]
    @IDChiNhanhs [uniqueidentifier],
    @IDNhanViens [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @KieuLuongs [nvarchar](20),
    @CurrentPage [int],
    @PageSize [float]
AS
BEGIN
    SET NOCOUNT ON;			

    		declare @IDNhanVienLogin uniqueidentifier= (select top 1 ID_NhanVien from HT_NguoiDung where LaAdmin='1')    
    		declare @ngaycongchuan float = (select dbo.TinhNgayCongChuan(@FromDate,@ToDate,@IDChiNhanhs))
    		
    		declare @tblCong CongThuCong
    		insert into @tblCong
    		exec dbo.GetChiTietCongThuCong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    		declare @tblThietLapLuong ThietLapLuong
    		insert into @tblThietLapLuong
    		exec GetNS_ThietLapLuong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    		declare @tblLuong table (LoaiLuong int, ID_NhanVien uniqueidentifier, LuongCoBan float, SoNgayDiLam float, LuongChinh float)				
    		insert into @tblLuong		
    		exec TinhLuongCoBan @ngaycongchuan, @tblCong, @tblThietLapLuong
    
    		declare @tblLuongOT table (ID_NhanVien uniqueidentifier, LuongOT float)				
    		insert into @tblLuongOT		
    		exec TinhLuongOT @ngaycongchuan, @tblCong, @tblThietLapLuong
    		
    		declare @tblPhuCap table (ID_NhanVien uniqueidentifier, PhuCapCoDinh float, PhuCapTheoNgayCong float)
    		insert into @tblPhuCap
    		exec TinhPhuCapLuong @tblCong, @tblThietLapLuong
    
    		declare @tblPhuCapTheoPtram table (ID_NhanVien uniqueidentifier,ID_PhuCap uniqueidentifier, TenPhuCap nvarchar(max),  PhuCapCoDinh float, HeSo float,
    		NgayApDung datetime, NgayKetThuc datetime, SoNgayDiLam float)
    		insert into @tblPhuCapTheoPtram
    		exec GetPhuCapCoDinh_TheoPtram @IDChiNhanhs, @FromDate, @ToDate, '%%',@tblCong, @tblThietLapLuong
    
    		declare @tblGiamTru table (ID_NhanVien uniqueidentifier, GiamTruCoDinhVND float, GiamTruTheoLan float, SoLanDiMuon float)
    		insert into @tblGiamTru
    		exec TinhGiamTruLuong @tblCong, @tblThietLapLuong	

    
    		-- get phucap codinh theo %luongchinh
    	declare @tblLuongPC table (ID_NhanVien uniqueidentifier,LoaiLuong int, LuongCoBan float, SoNgayDiLam float, LuongChinh float,PhuCapCoDinh_TheoPtramLuong float)						
    	insert into @tblLuongPC	
    	select 
    		pcluong.ID_NhanVien, pcluong.LoaiLuong, pcluong.LuongCoBan, pcluong.SoNgayDiLam, pcluong.LuongChinh, 
    		sum(PhuCapCoDinh_TheoPtramLuong) as PhuCapCoDinh_TheoPtramLuong
    	from
    		(select luong.ID_NhanVien, LoaiLuong, LuongCoBan, luong.SoNgayDiLam, LuongChinh,
    			case when PhuCapCoDinh is null then 0 else LuongChinh * PhuCapCoDinh * HeSo/100 end as PhuCapCoDinh_TheoPtramLuong
    		from @tblLuong luong
    		left join @tblPhuCapTheoPtram pc on luong.ID_NhanVien= pc.ID_NhanVien
    		) pcluong 
    		group by pcluong.ID_NhanVien, pcluong.LuongChinh, pcluong.LoaiLuong, pcluong.LuongCoBan,pcluong.SoNgayDiLam
    
    		
			---- ===== Tinhluong trong khoangthoigian

			declare @tblFromTo table (ID_BangLuong uniqueidentifier null, DateFrom datetime, DateTo datetime, isGiaoNhau int)

			select bl.ID, bl.MaBangLuong, bl.ID_DonVi, 
			 bl.TuNgay, bl.DenNgay
			into #tblTemp
			from NS_BangLuong bl
			where bl.TrangThai in (3,4)
			and ID_DonVi = @IDChiNhanhs
			and (
				(@FromDate <= TuNgay and @ToDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate >= TuNgay and @FromDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate >= TuNgay and @ToDate <= DenNgay and  @ToDate >= TuNgay)
				or (@FromDate <= TuNgay and  @ToDate >= DenNgay)
				)
	

		----====  get cac khoang thoi gian ----
				declare @cur_IDBangLuong uniqueidentifier, @cur_FDate datetime, @cur_TDate datetime
				declare _cur cursor
				for
					select ID, TuNgay, DenNgay
					from #tblTemp
				open _cur
				fetch next from _cur
				into @cur_IDBangLuong, @cur_FDate, @cur_TDate
				while @@FETCH_STATUS = 0
				begin
					
					if @FromDate < @cur_FDate
					begin
						if @ToDate < @cur_FDate
							insert into @tblFromTo values (null, @FromDate, @ToDate,0)
						else
							if @ToDate >= @cur_FDate and @ToDate < @cur_TDate
								insert into @tblFromTo values (null, @FromDate, @cur_FDate,0),
								(@cur_IDBangLuong, @cur_FDate, @ToDate,1)
							else
								insert into @tblFromTo values (null, @FromDate, @cur_FDate,0),
								(@cur_IDBangLuong, @cur_FDate, @cur_TDate,1),
								(null, @cur_TDate, @ToDate,0)								
					end
					else
					begin
						if @ToDate < @cur_TDate
							insert into @tblFromTo values (@cur_IDBangLuong, @FromDate, @ToDate,1)
						else
						insert into @tblFromTo values (@cur_IDBangLuong, @FromDate, @cur_TDate,1),
							(null, @cur_TDate, @ToDate,0)
					end
					fetch next from _cur into @cur_IDBangLuong, @cur_FDate, @cur_TDate
				end
				close _cur
				deallocate _cur

				drop table #tblTemp

				if (select count(*) from @tblFromTo) = 0
					insert into @tblFromTo values (null, @FromDate, @ToDate,0)

				------ ====== lay ds nhan vien thuoc (khong thuoc bang luong)
				
				declare @tblNhanVien table (ID_NhanVien uniqueidentifier)
				DECLARE @tab_HangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, 
				HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    			TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, 
				TongHoaHongBanGoiDV float, TongAll float)			

				DECLARE @tab_HoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    			TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)		

				DECLARE @tab_DoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    			Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float)     			

				---- temp
				DECLARE @temp_CKHangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, 
				HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    			TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, 
				TongHoaHongBanGoiDV float, TongAll float)

				DECLARE @temp_CKHoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    			TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)			

				DECLARE @temp_CKDoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    			Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float)     			   		

				declare @cur2_IDBangLuong uniqueidentifier, @cur2_FDate datetime, @cur2_TDate datetime, @cur2_isGiaoNhau int
				declare _cur2 cursor for
					select distinct * from @tblFromTo where DateFrom != DateTo	order by DateFrom
				open _cur2
				fetch next from _cur2
				into @cur2_IDBangLuong, @cur2_FDate, @cur2_TDate, @cur2_isGiaoNhau			
				while @@FETCH_STATUS =0
				begin			
					
					---- get nv exist bangluong				
					insert into @tblNhanVien
					select ct.ID_NhanVien
					from NS_BangLuong_ChiTiet ct
					join NS_BangLuong bl on ct.ID_BangLuong= bl.ID
					join NS_NhanVien nv on ct.ID_NhanVien= nv.ID
					where ct.ID_BangLuong = @cur2_IDBangLuong 					

					---- get ck hanghoa from-to
					insert into @temp_CKHangHoa
					exec ReportDiscountProduct_General @IDChiNhanhs, @IDNhanVienLogin,'%%','0,1,6,19,22,25', @cur2_FDate,@cur2_TDate, 16,1,0,100

					---- get ck hoadon from-to
					insert into @temp_CKHoaDon
					exec ReportDiscountInvoice @IDChiNhanhs,@IDNhanVienLogin,'%%','0,1,6,19,22,25', @cur2_FDate, @cur2_TDate, 8,1,0,0,100

					---- get ck doanhthu from-to
					insert into @temp_CKDoanhSo
					exec GetAll_DiscountSale @IDChiNhanhs,@IDNhanVienLogin, @cur2_FDate, @cur2_TDate, '%%', '', 0,1000		
				
					insert into @tab_HangHoa
					select * 
					from @temp_CKHangHoa ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)

					insert into @tab_HoaDon
					select * 
					from @temp_CKHoaDon ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)

					insert into @tab_DoanhSo
					select * 
					from @temp_CKDoanhSo ck
					where not exists (
						select nv.ID_NhanVien
						from @tblNhanVien nv where ck.ID_NhanVien = nv.ID_NhanVien
					)						

					---- neu da tao bangluong, update phucap codinh = 0
					if @cur2_IDBangLuong is not null
					begin
						update pc set pc.PhuCapCoDinh =0
						from @tblPhuCap pc
						where exists (
							select nv.ID_NhanVien
							from @tblNhanVien nv where pc.ID_NhanVien = nv.ID_NhanVien
						)

						update pc set pc.GiamTruCoDinhVND =0
						from @tblGiamTru pc
						where exists (
							select nv.ID_NhanVien
							from @tblNhanVien nv where pc.ID_NhanVien = nv.ID_NhanVien
						)
					end

					delete from @temp_CKHangHoa
					delete from @temp_CKHoaDon
					delete from @temp_CKDoanhSo
					delete from @tblNhanVien

					fetch next from _cur2
					into @cur2_IDBangLuong, @cur2_FDate, @cur2_TDate, @cur2_isGiaoNhau
				end
				close _cur2
				deallocate _cur2
    
    
    	-- hoahong		
    	--DECLARE @tab_DoanhSo TABLE (ID_NhanVien uniqueidentifier, MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), TongDoanhThu float, TongThucThu float, HoaHongDoanhThu float, HoaHongThucThu float, TongAll float,
    	--Status_DoanhThu nvarchar(10), TotalRow int, TotalPage float, TongAllDoanhThu float,TongAllThucThu float, TongHoaHongDoanhThu float, TongHoaHongThucThu float, TongAllAll float) 
    	--INSERT INTO @tab_DoanhSo exec GetAll_DiscountSale @IDChiNhanhs,@IDNhanVienLogin, @FromDate, @ToDate, '%%', '', 0,1000		
    
    	--DECLARE @tab_HoaDon TABLE (ID_NhanVien uniqueidentifier,MaNhanVien nvarchar(255), TenNhanVien nvarchar(max), HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float,
    	--TotalRow int, TotalPage float, TongHHDoanhThu float,TongHHThucThu float, TongHHVND float, TongAllAll float)
    	--INSERT INTO @tab_HoaDon exec ReportDiscountInvoice @IDChiNhanhs,@IDNhanVienLogin,'%%','0,1,6,19,22,25', @FromDate, @ToDate, 8,1,0,0,100
    
    	--DECLARE @tab_HangHoa TABLE (MaNhanVien nvarchar(255), TenNhanVien nvarchar(max),ID_NhanVien uniqueidentifier, HoaHongThucHien float, HoaHongThucHien_TheoYC float, HoaHongTuVan float, HoaHongBanGoiDV float, Tong float,
    	--TotalRow int, TotalPage float, TongHoaHongThucHien float,TongHoaHongThucHien_TheoYC float, TongHoaHongTuVan float, TongHoaHongBanGoiDV float, TongAll float)
    	--INSERT INTO @tab_HangHoa exec ReportDiscountProduct_General @IDChiNhanhs, @IDNhanVienLogin,'%%','0,1,6,19,22,25', @FromDate, @ToDate, 16,1,0,100
    
    	declare @tblHoaHong table (ID_NhanVien uniqueidentifier, TongDoanhThu float, HoaHong float, HoaHongHangHoa float, HoaHongHoaDon float, HoaHongDoanhThu float)
    	insert into  @tblHoaHong
    	SELECT a.ID_NhanVien, sum(TongDoanhThu) as TongDoanhThu,		
    		SUM(TongDoanhSo + TongHoaDon + TongHangHoa) as HoaHong,
    		SUM(TongHangHoa) as HoaHongHangHoa,
    		SUM(TongHoaDon) as HoaHongHoaDon,
    		SUM(TongDoanhSo) as HoaHongDoanhThu
    	FROM 
    	(
    		select ID_NhanVien,	Tong as TongHangHoa,0 as TongHoaDon, 0 as TongDoanhSo, 0 as TongDoanhThu					
    		from @tab_HangHoa
    		UNION ALL
    		Select ID_NhanVien,	0 as TongHangHoa,TongAll as TongHoaDon,	0 as TongDoanhSo, 0 as TongDoanhThu						
    		from @tab_HoaDon
    		UNION ALL
    		Select ID_NhanVien,	0 as TongHangHoa,0 as TongHoaDon,TongAll as TongDoanhSo, TongDoanhThu
    		from @tab_DoanhSo
    	) as a
    	GROUP BY a.ID_NhanVien
    
    	-- giamtru codinh %tongluongnhan
    	declare @tblGiamTruTheoPTram table (ID_NhanVien uniqueidentifier,ID_PhuCap uniqueidentifier, TenPhuCap nvarchar(max),  GiamTruCoDinh float, HeSo float,
    		NgayApDung datetime, NgayKetThuc datetime, SoLanDiMuon float)
    	insert into @tblGiamTruTheoPTram
    	exec GetGiamTruCoDinh_TheoPtram @IDChiNhanhs, @FromDate, @ToDate, '%%',@tblCong, @tblThietLapLuong	
    
		
    	select nv.MaNhanVien, nv.TenNhanVien, 
    			luong.*,		
    			cast(PhuCapCoBan + PhuCapKhac + PhuCapCoDinh_TheoPtramLuong as float) as PhuCap,				
    			cast(PhatDiMuon + GiamTruCoDinhVND as float) as TongTienPhat,
    			cast(LuongChinh as float)  as TongLuongNhan, -- save to DB
    			cast(LuongChinh + LuongOT +  PhuCapCoBan + PhuCapKhac + PhuCapCoDinh_TheoPtramLuong 
				+ KhenThuong + ChietKhau - PhatDiMuon - GiamTruCoDinhVND as float) as LuongThucNhan1
    	into #tblluong
    		from
    			(
    			select 
    				tbl.ID_NhanVien, 
    				max(tbl.LoaiLuong) as LoaiLuong,
    				max(tbl.LuongCoBan) as LuongCoBan,
    				ceiling(sum(tbl.LuongChinh)) as LuongChinh,
    				sum(LuongOT) as LuongOT,	
    				ceiling(sum(PhuCapCoDinh_TheoPtramLuong)) as PhuCapCoDinh_TheoPtramLuong,				
    				sum(PhuCapCoDinh) as PhuCapCoBan,
    				sum(PhuCapTheoNgay) as PhuCapKhac,
    				sum(GiamTruCoDinhVND) as GiamTruCoDinhVND,
    				sum(GiamTruTheoLan) as PhatDiMuon,
    				sum(HoaHong) as ChietKhau,
    				sum(HoaHongHangHoa) as HoaHongHangHoa,
    				sum(HoaHongHoaDon) as HoaHongHoaDon,
    				sum(HoaHongDoanhThu) as HoaHongDoanhThu,
    				sum(TongDoanhThu) as TongDoanhThu,
    				sum(SoNgayDiLam) as NgayCongThuc,
    				sum(SoGioOT) as SoGioOT,
    				sum(SoLanDiMuon) as SoLanDiMuon,
    				sum(KhenThuong) as KhenThuong,
    				@ngaycongchuan as  NgayCongChuan
    			from 
    				(select 
    					ID_NhanVien, LoaiLuong, LuongCoBan, LuongChinh,
    					cast(0 as float) as LuongOT, 
    					SoNgayDiLam, cast(0 as float) as SoGioOT, 
    					PhuCapCoDinh_TheoPtramLuong,
    					cast(0 as float) as PhuCapCoDinh, cast(0 as float) as PhuCapTheoNgay,
    					cast(0 as float) as GiamTruCoDinhVND, cast(0 as float) as GiamTruTheoLan, cast(0 as float) as SoLanDiMuon,
    					cast(0 as float) as HoaHong,cast(0 as float) as HoaHongHangHoa, cast(0 as float) as HoaHongHoaDon, cast(0 as float) as HoaHongDoanhThu, 0 as TongDoanhThu,
    					cast(0 as float) as KhenThuong
    				from @tblLuongPC
    
    				union all
    
    				select 
    					ID_NhanVien, 0 as LoaiLuong, 0 as LuongCoBan, 0 as LuongChinh,
    					LuongOT,
    					0 as SoNgayDiLam,0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgay,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					0 as HoaHong, 0 as HoaHongHangHoa,0 as HoaHongHoaDon, 0 as HoaHongDoanhThu,0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblLuongOT
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong,0 as LuongCoBan, 0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					PhuCapCoDinh, PhuCapTheoNgayCong,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					0 as HoaHong,0 as HoaHongHangHoa, 0 as HoaHongHoaDon, 0 as HoaHongDoanhThu,0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblPhuCap
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong,0 as LuongCoBan,0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT,
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgayCong,
    					GiamTruCoDinhVND, GiamTruTheoLan, SoLanDiMuon,
    					0 as HoaHong,0 as HoaHongHangHoa, 0 as HoaHongHoaDon, 0 as HoaHongDoanhThu, 0 as TongDoanhThu,
    					0 as KhenThuong
    				from @tblGiamTru
    
    				union all
    				select 
    					ID_NhanVien, 0 as LoaiLuong, 0 as LuongCoBan,0 as LuongChinh, 0 as LuongOT, 
    					0 as SoNgayDiLam, 0 as SoGioOT, 
    					0 as PhuCapCoDinh_TheoPtramLuong,
    					0 as PhuCapCoDinh, 0 as PhuCapTheoNgayCong,
    					0 as GiamTruCoDinhVND, 0 as GiamTruTheoLan, 0 as SoLanDiMuon,
    					HoaHong, HoaHongHangHoa, HoaHongHoaDon, HoaHongDoanhThu, TongDoanhThu,
    					0 as KhenThuong
    				from @tblHoaHong
    				) tbl group by tbl.ID_NhanVien
    			) luong
    		join NS_NhanVien nv on luong.ID_NhanVien= nv.ID
			where (nv.TrangThai= 1 or nv.TrangThai is null)
			and (nv.DaNghiViec= 0 or nv.DaNghiViec is null)
			
    		
    		if @IDNhanViens='' or 	@IDNhanViens='%%'	
    			select luong.* ,
					luong.LuongThucNhan1- ISNULL(ISNULL(gt.GiamTruCoDinh,0) * gt.HeSo * luong.TongLuongNhan/100,0) as LuongThucNhan,
    				ISNULL(ISNULL(gt.GiamTruCoDinh,0) * gt.HeSo * luong.TongLuongNhan/100,0) as GiamTruCoDinh_TheoPTram
    			from #tblluong luong
    			left join @tblGiamTruTheoPTram gt on luong.ID_NhanVien= gt.ID_NhanVien
    			where exists(select Name from dbo.splitstring(@KieuLuongs) kl where luong.LoaiLuong= kl.Name)
				and luong.LuongThucNhan1 != 0
    			order by luong.MaNhanVien
    		else
    			-- search by nhanvien
    			select luong.* ,
					luong.LuongThucNhan1- ISNULL(ISNULL(gt.GiamTruCoDinh,0) * gt.HeSo * luong.TongLuongNhan/100,0) as LuongThucNhan,
    				ISNULL(ISNULL(gt.GiamTruCoDinh,0) * gt.HeSo * luong.TongLuongNhan/100,0) as GiamTruCoDinh_TheoPTram
    			from #tblluong luong
    			left join @tblGiamTruTheoPTram gt on luong.ID_NhanVien= gt.ID_NhanVien
    			where exists(select Name from dbo.splitstring(@KieuLuongs) kl where luong.LoaiLuong= kl.Name )
    			and exists(select Name from dbo.splitstring(@IDNhanViens) nv where luong.ID_NhanVien= nv.Name)
    			order by luong.MaNhanVien
END");

			Sql(@"ALTER PROCEDURE [dbo].[update_DanhMucHangHoa]
AS
BEGIN
SET NOCOUNT ON;
    update DM_HangHoa set TenKhac = null
	update DM_HangHoa set ID_NhomHang = '00000000-0000-0000-0000-000000000000' where ID_NhomHang is null and LaHangHoa = 1
	update DM_HangHoa set ID_NhomHang = '00000000-0000-0000-0000-000000000001' where ID_NhomHang is null and LaHangHoa = 0
	exec insert_TonKhoKhoiTaoByInsert;
	If EXISTS(select * from BH_HoaDon_ChiTiet ct join BH_HoaDon bh on bh.ID = ct.ID_HoaDon where bh.SoLanIn = -9 and bh.ChoThanhToan = '0')
	BEGIN
		UPDATE hdkkupdate
    	SET hdkkupdate.TongTienHang = dshoadonkkupdate.SoLuongGiam, hdkkupdate.TongGiamGia = dshoadonkkupdate.SoLuongLech, hdkkupdate.TongChiPhi = dshoadonkkupdate.SoLuongTang
    	FROM BH_HoaDon AS hdkkupdate
    	INNER JOIN
    	(SELECT ct.ID_HoaDon, SUM(CASE WHEN ct.SoLuong > 0 THEN ct.SoLuong ELSE 0 END) AS SoLuongTang,
    	SUM(CASE WHEN ct.SoLuong < 0 THEN ct.SoLuong ELSE 0 END) AS SoLuongGiam, SUM(SoLuong) AS SoLuongLech FROM BH_HoaDon_ChiTiet ct
    	join BH_HoaDon hd on ct.ID_HoaDon = hd.ID and hd.SoLanIn = -9 and hd.ChoThanhToan = '0' GROUP BY ct.ID_HoaDon) AS dshoadonkkupdate
    	ON hdkkupdate.ID = dshoadonkkupdate.ID_HoaDon;
	END
	else
	BEGIN 
		Delete from BH_HoaDon where SoLanIn = -9 and ChoThanhToan = '0';
	END
	update BH_HoaDon set SoLanIn = NULL where SoLanIn = -9 and ChoThanhToan = '0'
	Delete from BH_HoaDon_ChiTiet where ID_HoaDon in (Select ID from BH_HoaDon where SoLanIn = -9 and ChoThanhToan = '1')
	Delete from BH_HoaDon where SoLanIn = -9 and ChoThanhToan = '1'
END");

            Sql(@"ALTER PROCEDURE [dbo].[ValueCard_ServiceUsed]
    @ID_ChiNhanhs [nvarchar](max),
    @TextSearch [nvarchar](max),
    @DateFrom [nvarchar](14),
    @DateTo [nvarchar](14),
    @Status [nvarchar](14),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;   
	declare @tblChiNhanh table (ID_Donvi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@ID_ChiNhanhs)

    		DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    		INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    		DECLARE @count int =  (Select count(*) from @tblSearchString);
    	
    		select hd.ID as ID_HoaDon,tblq.ID_HoaDon as ID_PhieuThuChi, hd.MaHoaDon,tblq.NgayLapHoaDon,ISNULL(dt.MaDoiTuong,'') as MaDoiTuong, ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong, 
    	qd.MaHangHoa,hh.TenHangHoa,ct.SoLuong, ct.DonGia, ct.TienChietKhau, ct.ThanhTien,  ISNULL(tblq.PhatSinhGiam,0) as PhatSinhGiam, ISNULL(tblq.PhatSinhTang,0) as PhatSinhTang, tblq.MaHoaDon as MaPhieuThu,		
    		case hd.LoaiHoaDon
    			when 1 then N'Bán hàng'
    			when 3 then N'Đặt hàng'
    			when 6 then N'Trả hàng'
    			when 19 then N'Gói dịch vụ'
    			when 25 then N'Sửa chữa'
    		else '' end as SLoaiHoaDon
    	from BH_HoaDon hd
    	join BH_HoaDon_ChiTiet ct on hd.id= ct.id_hoadon
    	left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    	join DonViQuiDoi qd on ct.id_donviquidoi= qd.id
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
    	join (select qct.ID_HoaDonLienQuan, MaHoaDon, NgayLapHoaDon, qct.ID_HoaDon,
    				case when qhd.LoaiHoaDon = 11 then SUM(ISNULL(qct.ThuTuThe ,0)) end as PhatSinhGiam,
    				case when qhd.LoaiHoaDon = 12 then SUM(ISNULL(qct.ThuTuThe ,0)) end as PhatSinhTang
    		from Quy_HoaDon_Chitiet qct 
    		join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
    		where qhd.TrangThai ='1' 
    			and qct.HinhThucThanhToan=4
    		and FORMAT(qhd.NgayLapHoaDon,'yyyy-MM-dd') >=@DateFrom
    		and FORMAT(qhd.NgayLapHoaDon,'yyyy-MM-dd') <= @DateTo
    		group by qct.ID_HoaDonLienQuan, qct.ID_HoaDon, qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.LoaiHoaDon) tblq on hd.ID= tblq.ID_HoaDonLienQuan
    	where hd.LoaiHoaDon in ( 1,3,6,19,25) 
    	and hd.ChoThanhToan ='0'
		and exists (select cn.ID_DonVi from @tblChiNhanh cn where hd.ID_DonVi= cn.ID_Donvi)
    	and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)	
    		order by hd.NgayLapHoaDon desc
END");

            CreateStoredProcedure(name: "[dbo].[BaoCaoGoiDV_GetCTMua]", parametersAction: p => new
            {
                IDChiNhanhs = p.String(),
                DateFrom = p.DateTime(),
                DateTo = p.DateTime()
            }, body: @"SET NOCOUNT ON;

	declare @tblChiNhanh table( ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@IDChiNhanhs)

	---- get gdvmua
	select 
		hd.MaHoaDon,
		hd.NgayLapHoaDon,
		hd.NgayApDungGoiDV,
		hd.HanSuDungGoiDV,
		hd.ID_DonVi,
		hd.ID_DoiTuong,
		ct.ID,
		ct.ID_HoaDon,
		ct.ID_DonViQuiDoi,
		ct.ID_LoHang,
		ct.SoLuong,
		ct.DonGia,
		ct.TienChietKhau,
		ct.ThanhTien,
		Case when hd.TongTienHang = 0 
		then 0 else ct.ThanhTien * ((hd.TongGiamGia + hd.KhuyeMai_GiamGia) / iif(hd.TongTienHang=0,1, hd.TongTienHang)) end as GiamGiaHD
	from BH_HoaDon hd
	join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
	where hd.LoaiHoaDon = 19
	and hd.ChoThanhToan=0
	and exists (select cn.ID_DonVi from @tblChiNhanh cn where cn.ID_DonVi= hd.ID_DonVi)
	and hd.NgayLapHoaDon between @DateFrom and @DateTo
	and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong= ct.ID)
	and (ct.ID_ParentCombo is null or ct.ID_ParentCombo!= ct.ID)");

            CreateStoredProcedure(name: "[dbo].[BCBanHang_GetChiPhi]", parametersAction: p => new
            {
                IDChiNhanhs = p.String(),
                DateFrom = p.DateTime(),
                DateTo = p.DateTime(),
                LoaiChungTus = p.String()
            }, body: @"SET NOCOUNT ON;

	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER)
    INSERT INTO @tblChiNhanh
    select Name from splitstring(@IDChiNhanhs);
    
    DECLARE @tblLoaiHoaDon TABLE(LoaiHoaDon int)
    INSERT INTO @tblLoaiHoaDon
    select Name from splitstring(@LoaiChungTus);

	select 		
		tbl.ID_ParentCombo,		
		ct.ID_DonViQuiDoi,
		tbl.ChiPhi,
		tbl.ID_NhanVien, 
		tbl.ID_DoiTuong		
	from
	(
		select 
			cpCT.ID_ParentCombo,
			cpCT.ID_NhanVien, 
			cpCT.ID_DoiTuong,				
			sum(cpCT.ThanhTien) as ChiPhi
		from
		(
			select 
				iif(ct.ID_ParentCombo is null, ct.ID, ct.ID_ParentCombo) as ID_ParentCombo, 						
				cp.ThanhTien,
				cp.ID_NhanVien, cp.ID_DoiTuong
			from
			(
				select cp.*,
					hd.MaHoaDon, hd.NgayLapHoaDon,
					hd.ID_NhanVien, hd.ID_DoiTuong					
				from BH_HoaDon_ChiPhi cp
				join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
				where hd.ChoThanhToan= 0
				and hd.NgayLapHoaDon between @DateFrom and @DateTo
				and exists (select ID from @tblChiNhanh cn where cn.ID = hd.ID_DonVi)
				and exists (select LoaiHoaDon from @tblLoaiHoaDon loai where loai.LoaiHoaDon = hd.LoaiHoaDon)
		   ) cp
		   join BH_HoaDon_ChiTiet ct on cp.ID_HoaDon_ChiTiet = ct.ID
		) cpCT group by cpCT.ID_ParentCombo,cpCT.ID_NhanVien, cpCT.ID_DoiTuong			
	)tbl
	join BH_HoaDon_ChiTiet ct on tbl.ID_ParentCombo = ct.ID");

            CreateStoredProcedure(name: "[dbo].[ChangePTN_updateCus]", parametersAction: p => new
            {
                ID_PhieuTiepNhan = p.Guid(),
                ID_KhachHangOld = p.Guid(),
                ID_BaoHiemOld = p.Guid(),
                Types = p.String(20)
            }, body: @"SET NOCOUNT ON;

	declare @tblType table(Loai int)
	insert into @tblType select name from dbo.splitstring(@Types)

	---- get PTN new
	declare @PTNNew_IDCusNew uniqueidentifier, @PTNNew_BaoHiem uniqueidentifier
	select @PTNNew_IDCusNew = ID_KhachHang, @PTNNew_BaoHiem = ID_BaoHiem from Gara_PhieuTiepNhan where ID= @ID_PhieuTiepNhan

	---- get list hoadon of PTN
	select ID, ID_DoiTuong, ID_BaoHiem
	into #tblHoaDon
	from BH_HoaDon
	where ID_PhieuTiepNhan = @ID_PhieuTiepNhan
	and ChoThanhToan =0
	and LoaiHoaDon in (3,25)

	---- update cus
	if (select count(*) from @tblType where Loai= '1') > 0
	begin
		update hd set ID_DoiTuong= @PTNNew_IDCusNew
		from BH_HoaDon hd
		join #tblHoaDon hdCheck on hd.ID= hdCheck.ID
		where hdCheck.ID_DoiTuong = @ID_KhachHangOld

		if (select count(*) from @tblType where Loai= '3') > 0
		update qct set ID_DoiTuong= @PTNNew_IDCusNew
		from Quy_HoaDon_ChiTiet qct
		join #tblHoaDon hdCheck on qct.ID_HoaDonLienQuan= hdCheck.ID
		where hdCheck.ID_DoiTuong = @ID_KhachHangOld
	end
  
	---- update baohiem
	if (select count(*) from @tblType where Loai= '2') > 0
	begin
		update hd set ID_BaoHiem= @PTNNew_BaoHiem
		from BH_HoaDon hd
		join #tblHoaDon hdCheck on hd.ID= hdCheck.ID
		where hdCheck.ID_BaoHiem = @ID_BaoHiemOld

		if (select count(*) from @tblType where Loai= '3') > 0
			update qct set ID_DoiTuong= @PTNNew_BaoHiem
			from Quy_HoaDon_ChiTiet qct
			join #tblHoaDon hdCheck on qct.ID_HoaDonLienQuan= hdCheck.ID
			where hdCheck.ID_BaoHiem = @ID_BaoHiemOld
	end");

            CreateStoredProcedure(name: "[dbo].[CTHD_GetChiPhiDichVu]", parametersAction: p => new
            {
                IDHoaDons = p.String(),
                IDVendors = p.String()
            }, body: @"SET NOCOUNT ON;
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
		left join BH_HoaDon_ChiPhi cp on cthd.ID= cp.ID_HoaDon_ChiTiet
		left join DonViQuiDoi qd on cthd.ID_DonViQuiDoi= qd.ID
	   left join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
	   left join DM_DoiTuong dt on cp.ID_NhaCungCap= dt.ID
	   left join Gara_DanhMucXe xe on hd.ID_Xe= xe.ID
	   ', @where,
	 ' order by qd.MaHangHoa ')

	 set @sql = concat(@tblDefined, @sql)

	 exec sp_executesql @sql, @paramDefined,
		@IDHoaDons_In = @IDHoaDons,
		@IDVendors_In = @IDVendors");

			CreateStoredProcedure(name: "[dbo].[GetChiPhiDichVu_byVendor]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				ID_NhaCungCap = p.String(),
				DateFrom = p.DateTime(),
				DateTo = p.DateTime(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
	declare @paramDefined nvarchar(max) ='@IDChiNhanhs_In nvarchar(max), 
											@IDNhaCungCap_In nvarchar(max),
											@DateFrom_In datetime,
											@DateTo_In datetime,
											@CurrentPage_In int,
											@PageSize_In int'
	declare @tblDefined nvarchar(max) = N' declare @tblChiNhanh table (ID uniqueidentifier) '

	declare @sqlTable varchar(max)= '', @sql nvarchar(max)= '', @sqlSoQuy nvarchar(max)= '', 
	@where nvarchar(max) ='', @whereNCC nvarchar(max)

	set @where= ' where 1 = 1 and hd.ChoThanhToan = 0 '
	set @whereNCC= ' where 1 = 1 '

	if isnull(@CurrentPage,'') ='' set @CurrentPage = 0
	if isnull(@PageSize,'') ='' set @PageSize = 10000

	if isnull(@IDChiNhanhs,'')!=''
		begin
			set @sqlTable = ' insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In)'
			set @where =concat(@where,' and exists (select cn.ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID)' )
		end

	if isnull(@ID_NhaCungCap,'')!=''
		begin			
			set @whereNCC = concat(@whereNCC,' and cp.ID_NhaCungCap = @IDNhaCungCap_In' )
			set @where =concat(@where,' and cp.ID_NhaCungCap = @IDNhaCungCap_In' )
		end

    if isnull(@DateFrom,'')!=''
		begin			
			set @where =concat(@where,' and hd.NgayLapHoaDon >= @DateFrom_In' )
		end

	if isnull(@DateTo,'')!=''
		begin			
			set @where =concat(@where,' and hd.NgayLapHoaDon < @DateTo_In' )
		end


		set @sql = concat(N'
		 ---- get hoadon co phidichvu
		select distinct cp.ID_HoaDon, cp.ID_NhaCungCap into #tblChiPhi
		from BH_HoaDon_ChiPhi cp ', @whereNCC , ' ;',

		N' 
		with data_cte
				as(
					select 
						hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, 
						125 as LoaiHoaDon,
						cp.ThanhTien as TongChiphi,
						isnull(thuchi.TienThu,0) as DaThanhToan,
						cp.ThanhTien - isnull(thuchi.TienThu,0) as ConNo,
						thuchi.ID_PhieuChi,
						thuchi.MaPhieuChi,
						case hd.LoaiHoaDon 
							when 1 then N''Bán lẻ''
							when 4 then N''Nhập hàng''
							when 6 then N''Trả hàng''
							when 7 then N''Trả hàng nhập''
							when 19 then N''Gói dịch vụ''
							when 25 then N''Sửa chữa''
						end as strLoaiHoaDon
					from
					(
						SELECT cp.ID_HoaDon, cp.ID_NhaCungCap,				
							sum(cp.ThanhTien) as ThanhTien						
						from BH_HoaDon_ChiPhi cp
						join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
						join BH_HoaDon_ChiTiet ct on cp.ID_HoaDon_ChiTiet = ct.ID ',
						@where,
						' group by cp.ID_HoaDon, cp.ID_NhaCungCap
					)cp
					join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
					left join
					(
						select	
							cp.ID_HoaDon,  ct.ID_DoiTuong, 								
							max(qhd.MaHoaDon) as MaPhieuChi,
							max(qhd.ID) as ID_PhieuChi,
							sum(TienThu) as TienThu
						from Quy_HoaDon_ChiTiet ct
						join Quy_HoaDon qhd on ct.ID_HoaDon= qhd.ID
						join #tblChiPhi cp on ct.ID_HoaDonLienQuan = cp.ID_HoaDon and cp.ID_NhaCungCap = ct.ID_DoiTuong
						where qhd.TrangThai = 1
						group by cp.ID_HoaDon,  ct.ID_DoiTuong
					) thuchi on cp.ID_HoaDon= thuchi.ID_HoaDon
				),
			count_cte
			as (
			select count(ID) as TotalRow,
				--CEILING(COUNT(ID) / CAST(@PageSize_In as float ))  as TotalPage,
				sum(TongChiphi) as SumTongTienHang,
				sum(DaThanhToan) as SumDaThanhToan,
				sum(ConNo) as SumConNo
			from data_cte dt
			)
			select dt.*, cte.*	
			from data_cte dt
			cross join count_cte cte
			order by dt.NgayLapHoaDon desc
			OFFSET (@CurrentPage_In* @PageSize_In) ROWS
		  FETCH NEXT @PageSize_In ROWS ONLY
			 ')
	set @sql= CONCAT(@tblDefined, @sqlTable,@sql)

	print @sql

	exec sp_executesql @sql,@paramDefined,
		@IDChiNhanhs_In = @IDChiNhanhs,
		@IDNhaCungCap_In = @ID_NhaCungCap,
    	@DateFrom_In = @DateFrom,
    	@DateTo_In = @DateTo,
    	@CurrentPage_In = @CurrentPage,
    	@PageSize_In = @PageSize");

			CreateStoredProcedure(name: "[dbo].[PTN_CheckChangeCus]", parametersAction: p => new
			{
				ID_PhieuTiepNhan = p.Guid(),
				ID_KhachHangNew = p.Guid(),
				ID_BaoHiemNew = p.String(40)
			}, body: @"SET NOCOUNT ON;

	if isnull(@ID_BaoHiemNew,'')=''
		set @ID_BaoHiemNew ='00000000-0000-0000-0000-000000000000'

	declare @tblReturn table(Loai int)

	---- get PTN old
	declare @PTNOld_IDCus uniqueidentifier, @PTNOld_BaoHiem uniqueidentifier
	select @PTNOld_IDCus = ID_KhachHang, @PTNOld_BaoHiem = ID_BaoHiem from Gara_PhieuTiepNhan where ID= @ID_PhieuTiepNhan

	---- get list hoadon of PTN
	select ID, ID_DoiTuong, ID_BaoHiem
	into #tblHoaDon
	from BH_HoaDon
	where ID_PhieuTiepNhan = @ID_PhieuTiepNhan
	and ChoThanhToan =0
	and LoaiHoaDon in (3,25)


	if @ID_KhachHangNew != @PTNOld_IDCus
	begin
		declare @count1 int;
		select @count1 =count(*)
		from #tblHoaDon
		where ID_DoiTuong != @ID_KhachHangNew

		if @count1 > 0
			insert into @tblReturn values (1)
	end

	if isnull(@PTNOld_BaoHiem,'00000000-0000-0000-0000-000000000000')='00000000-0000-0000-0000-000000000000'
		set @PTNOld_BaoHiem ='00000000-0000-0000-0000-000000000000'

  
	if @ID_BaoHiemNew != @PTNOld_BaoHiem
	begin
		declare @count2 int;
		select @count2 =count(*)
		from #tblHoaDon
		where isnull(ID_BaoHiem,'00000000-0000-0000-0000-000000000000') != @ID_BaoHiemNew
		
		if @count2 > 0
			insert into @tblReturn values (2)
	end

	---- check exist soquy
	declare @countSQ int
	select @countSQ = count(qhd.ID)
	from #tblHoaDon hd
	join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan
	join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
	where qhd.TrangThai is null or qhd.TrangThai= 1

	if @countSQ > 0
	 insert into @tblReturn values (3)

	 select * from @tblReturn");

			CreateStoredProcedure(name: "[dbo].[TheGiaTri_GetLichSuNapTien]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				ID_Cutomer = p.String(),
				TextSearch = p.String(),
				DateFrom = p.String(),
				DateTo = p.String(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
	
	declare @paramIn nvarchar(max)=' declare @isNull_txtSearch int = 1 '

	declare @tblDefined nvarchar(max)='', @sql1 nvarchar(max) ='',  @sql2 nvarchar(max) ='',
	@whereIn nvarchar(max)='', @whereOut nvarchar(max)='',
	
	@paramDefined nvarchar(max)= N'
			@IDChiNhanhs_In [nvarchar](max) ,
			@ID_Cutomer_In [nvarchar](max),
			@TextSearch_In [nvarchar](max),
			@DateFrom_In [datetime],
			@DateTo_In [datetime],		
			@CurrentPage_In [int],
			@PageSize_In [int]
			 '
		set @whereIn = ' where 1 = 1 and hd.LoaiHoaDon in (22,23)'
		set @whereOut = ' where 1 = 1'

		if isnull(@CurrentPage,'') ='' set @CurrentPage = 0
		if isnull(@PageSize,'') ='' set @PageSize = 20

		if isnull(@IDChiNhanhs,'')!=''
			begin
				set @tblDefined = concat(@tblDefined, N' declare @tblChiNhanh table (ID uniqueidentifier)
					insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In) ')
				set @whereIn= CONCAT(@whereIn, ' and exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID)')
			end

		if isnull(@ID_Cutomer,'')!=''
		begin
			set @whereIn= CONCAT(@whereIn, ' and hd.ID_DoiTuong = @ID_Cutomer_In')
		end

	   if isnull(@DateFrom,'')!=''
		begin
			set @whereIn= CONCAT(@whereIn, ' and hd.NgayLapHoaDon >= @DateFrom_In')
		end

		if isnull(@DateTo,'')!=''
		begin
			set @whereIn= CONCAT(@whereIn, ' and hd.NgayLapHoaDon < @DateTo_In')
		end

		if isnull(@TextSearch,'')!='' and isnull(@TextSearch,'')!='%%'
			begin			
				set @paramIn = CONCAT(@paramIn, ' set @isNull_txtSearch = 0')
				set @TextSearch = CONCAT(N'%', @TextSearch, '%')
				set @whereOut= CONCAT(@whereOut, ' 
					and (MaHoaDon like @TextSearch_In
						OR MaDoiTuong like @TextSearch_In
						OR TenDoiTuong like @TextSearch_In
						OR TenDoiTuong_KhongDau like @TextSearch_In
						OR MaHoaDon like @TextSearch_In
						OR DienGiai like @TextSearch_In
						OR DienGiaiUnSign like @TextSearch_In)'
					)					
			end

			set @sql1 = concat(N'

			select hd.ID,
				hd.ID_DoiTuong,			
                hd.MaHoaDon,
                hd.LoaiHoaDon,
                hd.NgayLapHoaDon,
                hd.TongChiPhi,
                hd.TongChietKhau,
                hd.TongTienHang,
                hd.TongTienThue,
                hd.TongGiamGia,
                hd.PhaiThanhToan,                                     
                hd.DienGiai
				into #htThe
			from BH_HoaDon hd ', @whereIn)
			

		
			set @sql2 = concat(N'
		with data_cte
    	as(
			select tbl.*, 
				dt.MaDoiTuong as MaKhachHang,
				dt.TenDoiTuong as TenKhachHang
				from
				(
				select hd.ID,
					hd.ID_DoiTuong,
					hd.LoaiHoaDon,
					hd.MaHoaDon,
					hd.NgayLapHoaDon,
					hd.TongChiPhi as MucNap,
					hd.TongChietKhau as KhuyenMaiVND,
					hd.TongTienHang as TongTienNap,
					hd.TongTienHang + hd.TongChietKhau as SoDuSauNap,
					hd.TongGiamGia as ChietKhauVND,
					hd.PhaiThanhToan,
					hd.DienGiai,
					iif(@isNull_txtSearch =0, dbo.FUNC_ConvertStringToUnsign(hd.DienGiai), hd.DienGiai ) as DienGiaiUnSign,
					isnull(thu.KhachDaTra,0) as KhachDaTra
				from #htThe hd
				left join
				(
					select 
						qct.ID_HoaDonLienQuan,
						sum(qct.TienThu) as KhachDaTra
					from Quy_HoaDon_ChiTiet qct
					join #htThe hd on qct.ID_HoaDonLienQuan = hd.ID
					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
					where qhd.TrangThai= 1 or qhd.TrangThai= 1
					group by qct.ID_HoaDonLienQuan
				) thu
				on hd.ID = thu.ID_HoaDonLienQuan
			) tbl 
			join DM_DoiTuong dt on tbl.ID_DoiTuong = dt.ID
			', @whereOut ,
			'),
			count_cte
				as (
				select count(dt.ID) as TotalRow,
    				CEILING(count(dt.ID) / cast(@PageSize_In as float)) as TotalPage,
    				sum(TongTienNap) as SumTongTienNap,
    				sum(SoDuSauNap) as SumSoDuSauNap,
    				sum(PhaiThanhToan) as SumPhaiThanhToan,
    				sum(KhachDaTra) as SumKhachDaTra
				from data_cte dt
    		)
    		select*
			from data_cte dt
			cross join count_cte
			order by dt.NgayLapHoaDon desc
			OFFSET(@CurrentPage_In * @PageSize_In) ROWS
			FETCH NEXT @PageSize_In ROWS ONLY
		')

		set @sql2= CONCAT(@paramIn, '; ', @tblDefined, @sql1,'; ', @sql2)		

		print @sql2

		exec sp_executesql @sql2, 
    		@paramDefined,
    		@IDChiNhanhs_In= @IDChiNhanhs,
			@ID_Cutomer_In = @ID_Cutomer,
			@TextSearch_In = @TextSearch,
			@DateFrom_In = @DateFrom,
			@DateTo_In = @DateTo,			
			@CurrentPage_In = @CurrentPage,
			@PageSize_In = @PageSize");
        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[BaoCaoGoiDV_GetCTMua]");
            DropStoredProcedure("[dbo].[BCBanHang_GetChiPhi]");
            DropStoredProcedure("[dbo].[ChangePTN_updateCus]");
            DropStoredProcedure("[dbo].[CTHD_GetChiPhiDichVu]");
            DropStoredProcedure("[dbo].[GetChiPhiDichVu_byVendor]");
			DropStoredProcedure("[dbo].[PTN_CheckChangeCus]");
			DropStoredProcedure("[dbo].[TheGiaTri_GetLichSuNapTien]");
        }
    }
}
