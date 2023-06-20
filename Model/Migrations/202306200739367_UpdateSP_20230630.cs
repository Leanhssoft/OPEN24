namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    //20230620
    public partial class UpdateSP_20230630 : DbMigration
    {
        public override void Up()
        {
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
						--+ isnull((select dbo.BuTruTraHang_HDDoi(hd.ID_HoaDon, hd.NgayLapHoaDon, hdgoc.ID_HoaDon, hdgoc.LoaiHoaDon)),0) --- hdTra tructiep cua hdDoi + hdGoc
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
END
");

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
END
");

			Sql(@"ALTER proc [dbo].[getlist_HoaDonBanHang]
	@timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @maHD [nvarchar](max),
	@ID_NhanVienLogin nvarchar(max),
	@NguoiTao nvarchar(max),
	@IDViTris nvarchar(max),
	@IDBangGias nvarchar(max),
	@TrangThai nvarchar(max),
	@PhuongThucThanhToan nvarchar(max)='',
	@ColumnSort varchar(max),
	@SortBy varchar(max),
	@CurrentPage int,
	@PageSize int,
	@LaHoaDonSuaChua nvarchar(10),
	@BaoHiem int
As

Begin
set nocount on;


--declare @ID_ChiNhanh nvarchar(max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
--@timeStart datetime='2022-03-01', @timeEnd datetime ='2023-12-30',
--@IDViTris nvarchar(max) ='',
--@IDBangGias nvarchar(max) ='',
--@LaHoaDonSuaChua varchar(20)='1,25',
--@BaoHiem int = 3,
--@TrangThai nvarchar(max) ='0,1,2,3',
--@maHD [nvarchar](max) ='CG.01_HDBL0191',
--@PhuongThucThanhToan nvarchar(max) ='0,1,2,3,4,5',
--@ColumnSort nvarchar(max) ='NgayLapHoaDon',
--@SortBy nvarchar(max) ='DESC',
--@CurrentPage int = 0,
--@PageSize int = 50,
--@ID_NhanVienLogin uniqueidentifier='ae260e5c-fdac-4711-afaf-4a0df5a2a979',
--@NguoiTao nvarchar(max)='admin'

	 declare @tblNhanVien table (ID uniqueidentifier)
	insert into @tblNhanVien
	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanh,'HoaDon_XemDS_PhongBan','HoaDon_XemDS_HeThong');

	
	declare @tblChiNhanh table (ID uniqueidentifier)
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@ID_ChiNhanh) where Name!=''

	declare @tblPhuongThuc table (PhuongThuc int)
	insert into @tblPhuongThuc
	select Name from dbo.splitstring(@PhuongThucThanhToan)
	

	declare @tblTrangThai table (TrangThaiHD tinyint NOT NULL PRIMARY KEY)
	insert into @tblTrangThai
	select cast(Name as tinyint) from dbo.splitstring(@TrangThai);

	declare @tblViTri table (ID uniqueidentifier)
	insert into @tblViTri
	select Name from dbo.splitstring(@IDViTris) where Name !=''
	

	declare @tblBangGia table (ID uniqueidentifier)
	insert into @tblBangGia
	select Name from dbo.splitstring(@IDBangGias) where Name !=''

	declare @tblLoaiHoaDon table (Loai int)
	insert into @tblLoaiHoaDon
	select Name from dbo.splitstring(@LaHoaDonSuaChua)

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);

				

			----- get list hd from - to
			select hd.ID, hd.ID_HoaDon, hd.ID_DoiTuong, hd.ID_BaoHiem, hd.NgayLapHoaDon, hd.ChoThanhToan				
			into #tmpHoaDon
			from BH_HoaDon hd
			where  hd.NgayLapHoadon between @timeStart and @timeEnd					
			and exists (select loai.Loai from @tblLoaiHoaDon loai where hd.LoaiHoaDon = loai.Loai)
			and exists (select cn.ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID) 
			and exists (select * from @tblNhanVien nv where nv.ID= hd.ID_NhanVien or hd.NguoiTao= @NguoiTao)				

				;with data_cte
				as(		
				select c.* ,
					------ những hóa đơn lâu đời, chưa có trường TongThanhToan = 0/null --> assign TongThanhToan = PhaiThanhToan ---
					iif(c.TongThanhToan1 =0 and c.PhaiThanhToan> 0, c.PhaiThanhToan, c.TongThanhToan1) as TongThanhToan,
					isnull(iif(c.LoaiHoaDonGoc = 3 or c.ID_HoaDon is null,
						iif(c.KhachNo <= 0, 0, ---  khachtra thuatien --> công nợ âm
							case when c.TongGiaTriTra > c.KhachNo then c.KhachNo						
							else c.TongGiaTriTra end),
						(select dbo.BuTruTraHang_HDDoi(ID_HoaDon,NgayLapHoaDon,ID_HoaDonGoc, LoaiHoaDonGoc))				
					),0) as LuyKeTraHang
			
				from
				(
				select 
					hd.ID,
					hd.ID_DonVi,
					hd.ID_DoiTuong,
					hd.ID_HoaDon,
					hd.ID_BaoHiem,
					hd.ID_PhieuTiepNhan,
					hd.ID_KhuyenMai,
					hd.ID_NhanVien,
					hd.ID_Xe,
					hd.ChoThanhToan,
					hd.MaHoaDon,
					hd.LoaiHoaDon,
					hd.NgayLapHoaDon,
					hd.KhuyenMai_GhiChu,
					isnull(hd.KhuyeMai_GiamGia,0) as KhuyeMai_GiamGia,
					isnull(hd.DiemGiaoDich,0) as DiemGiaoDich,
					isnull(hd.TongThueKhachHang,0) as  TongThueKhachHang,
					isnull(hd.CongThucBaoHiem,0) as  CongThucBaoHiem,
					isnull(hd.GiamTruThanhToanBaoHiem,0) as  GiamTruThanhToanBaoHiem,
					isnull(hd.PTThueHoaDon,0) as  PTThueHoaDon,
					isnull(hd.TongTienThueBaoHiem,0) as  TongTienThueBaoHiem,
					isnull(hd.TongTienBHDuyet,0) as  TongTienBHDuyet,
					isnull(hd.SoVuBaoHiem,0) as  SoVuBaoHiem,
					isnull(hd.PTThueBaoHiem,0) as  PTThueBaoHiem,
					isnull(hd.KhauTruTheoVu,0) as  KhauTruTheoVu,
					isnull(hd.GiamTruBoiThuong,0) as  GiamTruBoiThuong,
					isnull(hd.PTGiamTruBoiThuong,0) as  PTGiamTruBoiThuong,
					isnull(hd.BHThanhToanTruocThue,0) as  BHThanhToanTruocThue,
					ISNULL(hd.ID_BangGia,N'00000000-0000-0000-0000-000000000000') as ID_BangGia,
					ISNULL(hd.ID_ViTri,N'00000000-0000-0000-0000-000000000000') as ID_ViTri,

					CASE 
    					WHEN dt.TheoDoi IS NULL THEN 
    						CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    					ELSE dt.TheoDoi
    					END AS TheoDoi,

					dt.MaDoiTuong,
					dt.NgaySinh_NgayTLap,
					dt.MaSoThue,
					dt.TaiKhoanNganHang,
					ISNULL(dt.TongTichDiem,0) AS DiemSauGD,
					ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
					ISNULL(dt.Email, N'') as Email,
					ISNULL(dt.DienThoai, N'') as DienThoai,
					ISNULL(dt.DiaChi, N'') as DiaChiKhachHang,
					isnull(bh.MaDoiTuong,'') as MaBaoHiem,
					isnull(bh.TenDoiTuong,'') as TenBaoHiem,
					isnull(bh.DienThoai,'') as BH_SDT,
					isnull(bh.DiaChi,'') as BH_DiaChi,
					isnull(bh.Email,'') as BH_Email,
		
					dt.ID_TinhThanh, 
					dt.ID_QuanHuyen,
					ISNULL(tt.TenTinhThanh, N'') as KhuVuc,
					ISNULL(qh.TenQuanHuyen, N'') as PhuongXa,
					ISNULL(dv.TenDonVi, N'') as TenDonVi,
					ISNULL(dv.DiaChi, N'') as DiaChiChiNhanh,
					ISNULL(dv.SoDienThoai, N'') as DienThoaiChiNhanh,
					ISNULL(nv.TenNhanVien, N'') as TenNhanVien,
    	
    				ISNULL(gb.TenGiaBan,N'Bảng giá chung') AS TenBangGia,
					ISNULL(vt.TenViTri,'') as TenPhongBan,
		
					hd.DienGiai,
					hd.NguoiTao as NguoiTaoHD,
					ISNULL(hd.TongChietKhau,0) as TongChietKhau,
					ISNULL(hd.TongTienHang,0) as TongTienHang,
					ISNULL(hd.ChiPhi,0) as TongChiPhi, --- chiphi cuahang phaitra
					ISNULL(hd.TongGiamGia,0) as TongGiamGia,
					ISNULL(hd.TongTienThue,0) as TongTienThue,
					ISNULL(hd.PhaiThanhToan,0) as PhaiThanhToan,
					ISNULL(hd.TongThanhToan,0) as TongThanhToan1,
					ISNULL(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem,
	
					iif(hd.ID_BaoHiem is null, 2, 1) as SuDungBaoHiem,
		
					ISNULL(hdSq.TienMat,0) as TienMat,
					ISNULL(hdSq.TienATM,0) as TienATM,
					ISNULL(hdSq.ChuyenKhoan,0) as ChuyenKhoan,
					ISNULL(hdSq.TienDoiDiem,0) as TienDoiDiem,
					ISNULL(hdSq.ThuTuThe,0) as ThuTuThe,
					ISNULL(hdSq.TienDatCoc,0) as TienDatCoc,
					ISNULL(hdSq.KhachDaTra,0) as KhachDaTra,
					ISNULL(hdSq.BaoHiemDaTra,0) as BaoHiemDaTra,
					ISNULL(hdSq.DaThanhToan,0) as DaThanhToan,
					ISNULL(hdSq.ThuDatHang,0) as ThuDatHang,
					ISNULL(hd.PhaiThanhToan,0) - ISNULL(hdSq.KhachDaTra,0) as KhachNo,
					
					isnull(hdgoc.LoaiHoaDon,0) as LoaiHoaDonGoc,
					hdgoc.ID_HoaDon as ID_HoaDonGoc,					
					hdgoc.MaHoaDon as MaHoaDonGoc,
					ISNULL(allTra.TongGtriTra,0) as TongGiaTriTra,
					ISNULL(allTra.NoTraHang,0) as NoTraHang,


					cthd.GiamGiaCT,
					cthd.ThanhTienChuaCK,
					isnull(cthd.GiaTriSDDV,0) as GiaTriSDDV,

					tn.MaPhieuTiepNhan,
					cx.MaDoiTuong as MaChuXe,
					cx.TenDoiTuong as ChuXe,
					isnull(xe.BienSo,'') as BienSo,
					xe.ID_KhachHang as ID_ChuXe,

						Case When hd.ChoThanhToan = '1' then N'Phiếu tạm' when hd.ChoThanhToan = '0' then N'Hoàn thành' else N'Đã hủy' end as TrangThai,
						case  hd.ChoThanhToan
							when 1 then '1'
							when 0 then '0'
						else '4' end as TrangThaiHD,
						iif(hd.ID_PhieuTiepNhan is null, '0','1') as LaHoaDonSuaChua,
						case when hdSq.TienMat > 0 then
							case when hdSq.TienATM > 0 then	
								case when hdSq.ChuyenKhoan > 0 then
									case when hdSq.ThuTuThe > 0 then '1,2,3,4' else '1,2,3' end												
									else 
										case when hdSq.ThuTuThe > 0 then  '1,2,4' else '1,2' end end
									else
										case when hdSq.ChuyenKhoan > 0 then 
											case when hdSq.ThuTuThe > 0 then '1,3,4' else '1,3' end
											else 
													case when hdSq.ThuTuThe > 0 then '1,4' else '1' end end end
							else
								case when hdSq.TienATM > 0 then
									case when hdSq.ChuyenKhoan > 0 then
											case when hdSq.ThuTuThe > 0 then '2,3,4' else '2,3' end	
											else 
												case when hdSq.ThuTuThe > 0 then '2,4' else '2' end end
										else 		
											case when hdSq.ChuyenKhoan > 0 then
												case when hdSq.ThuTuThe > 0 then '3,4' else '3' end
												else 
												case when hdSq.ThuTuThe > 0 then '4' else '5' end end end end
									
									as PTThanhToan
				from
				(
					Select 
    					soquy.ID_HoaDonLienQuan,   				
						SUM(ISNULL(soquy.ThuTuThe, 0)) as ThuTuThe,
						SUM(ISNULL(soquy.TienMat, 0)) as TienMat,
						SUM(ISNULL(soquy.TienATM, 0)) as TienATM,
						SUM(ISNULL(soquy.TienCK, 0)) as ChuyenKhoan,
						SUM(ISNULL(soquy.TienDoiDiem, 0)) as TienDoiDiem,
						SUM(ISNULL(soquy.TienDatCoc, 0)) as TienDatCoc,
						SUM(ISNULL(soquy.TienThu, 0)) as DaThanhToan,
						SUM(ISNULL(soquy.KhachDaTra, 0)) as KhachDaTra,
						SUM(ISNULL(soquy.ThuDatHang, 0)) as ThuDatHang,
						SUM(ISNULL(soquy.BaoHiemDaTra, 0)) as BaoHiemDaTra
    				from
    				(
						Select 
							hd.ID as ID_HoaDonLienQuan,	
							iif(qhd.TrangThai='0',0, case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=1, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=1, -qct.TienThu,0) end) as TienMat,
							iif(qhd.TrangThai='0',0,case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=2, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=2, -qct.TienThu,0) end) as TienATM,
							iif(qhd.TrangThai='0',0,case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=3, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=3, -qct.TienThu,0) end) as TienCK,
							iif(qhd.TrangThai='0',0,case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=5, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=5, -qct.TienThu,0) end) as TienDoiDiem,
							iif(qhd.TrangThai='0',0,case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=4, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=4, -qct.TienThu,0) end) as ThuTuThe,
							iif(qhd.TrangThai='0',0,case when qhd.LoaiHoaDon = 11 then iif(qct.HinhThucThanhToan=6, qct.TienThu,0) else  iif(qct.HinhThucThanhToan=6, -qct.TienThu,0) end) as TienDatCoc,
							iif(qhd.TrangThai='0',0,iif(qhd.LoaiHoaDon = 11, qct.TienThu, - qct.TienThu)) as TienThu,
							iif(qhd.TrangThai='0',0,iif(dt.LoaiDoiTuong =1, iif(qhd.LoaiHoaDon = 11, qct.TienThu, - qct.TienThu),0)) as KhachDaTra,
							0 as ThuDatHang,
							iif(qhd.TrangThai='0',0,iif(dt.LoaiDoiTuong =3, iif(qhd.LoaiHoaDon = 11, qct.TienThu, - qct.TienThu),0)) as BaoHiemDaTra						
						from #tmpHoaDon hd
						left join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan	
						left join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID 
						left join DM_DoiTuong dt on qct.ID_DoiTuong = dt.ID			
						where hd.ChoThanhToan='0'
						

						union all

						Select
							thuDH.ID,
							thuDH.TienMat,
							thuDH.TienATM,
							thuDH.ChuyenKhoan,
							thuDH.TienDoiDiem,
							thuDH.ThuTuThe,
							thuDH.TienDatCoc,
							thuDH.TienThu,
							thuDH.TienThu as KhachDaTra,
							thuDH.TienThu as ThuDatHang,
							0 as BaoHiemDaTra
						FROM
						(
							Select 
									ROW_NUMBER() OVER(PARTITION BY d.ID_HoaDon ORDER BY d.NgayLapHoaDon ASC) AS isFirst,						
    								d.ID,
									d.ID_HoaDon,
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
								select 
									hd.ID,
									hd.NgayLapHoaDon,
									hdd.ID as ID_HoaDon,
									iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
									iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
									iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
									iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
									iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
									iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu,
									iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc	
								from #tmpHoaDon hd								
								join BH_HoaDon hdd on hd.ID_HoaDon= hdd.ID	and hdd.LoaiHoaDon= 3				
								join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDonLienQuan = hdd.ID
								join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
								where (qhd.TrangThai= 1 Or qhd.TrangThai is null)										
								and hd.ChoThanhToan='0'	
							)  d group by d.ID,d.NgayLapHoaDon,ID_HoaDon		
						) thuDH where isFirst= 1
					) soquy group by soquy.ID_HoaDonLienQuan
			) hdSq
			join BH_HoaDon hd on hdSq.ID_HoaDonLienQuan = hd.ID
			left join BH_HoaDon hdgoc on hd.ID_HoaDon= hdgoc.ID
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
			) allTra on allTra.ID_HoaDon = hd.ID
			left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
			left join DM_DonVi dv on hd.ID_DonVi = dv.ID
			left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID 
			left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
			left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
			left join DM_GiaBan gb on hd.ID_BangGia = gb.ID
			left join DM_ViTri vt on hd.ID_ViTri = vt.ID
			left join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan = tn.ID
			left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID	
			left join DM_DoiTuong bh on hd.ID_BaoHiem = bh.ID and bh.LoaiDoiTuong = 3
			left join DM_DoiTuong cx on xe.ID_KhachHang= cx.ID					
			left join
			(
		
				select 
					cthd.ID_HoaDon,
					sum(GiamGiaCT) as GiamGiaCT,
					sum(ThanhTienChuaCK) as ThanhTienChuaCK,
					sum(GiaTriSDDV) as GiaTriSDDV
				from
				(
						------- cthd -----------
				select 
					ct.ID_HoaDon,
					ct.SoLuong * ct.TienChietKhau as GiamGiaCT,
					ct.SoLuong * ct.DonGia  as ThanhTienChuaCK,
					0 as GiaTriSDDV
				from #tmpHoaDon hd
				join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon				
				where	(ct.ID_ChiTietDinhLuong= ct.ID or ct.ID_ChiTietDinhLuong is null)
						and (ct.ID_ParentCombo= ct.ID or ct.ID_ParentCombo is null)		

				union all

				------ ctsudung ---
				select 
					ctsd.ID_HoaDon,
					0 as GiamGiaCT,
					0 as ThanhTienChuaCK,
					ctsd.SoLuong * (ct.DonGia - ct.TienChietKhau) * ( 1 -  gdv.TongGiamGia/iif(gdv.TongTienHang =0,1,gdv.TongTienHang))  as GiaTriSDDV
				from  #tmpHoaDon hdsd
				join BH_HoaDon_ChiTiet ctsd on ctsd.ID_HoaDon= hdsd.ID 		
				join BH_HoaDon_ChiTiet ct on ctsd.ID_ChiTietGoiDV = ct.ID 
				join BH_HoaDon gdv on ct.ID_HoaDon= gdv.ID and gdv.LoaiHoaDon = 19				
				where (ctsd.ID_ChiTietDinhLuong= ctsd.ID or ctsd.ID_ChiTietDinhLuong is null)		
				and ctsd.ID_ChiTietGoiDV is not null
				
				) cthd group by cthd.ID_HoaDon
			) cthd on hd.ID = cthd.ID_HoaDon
			where 
			(@IDViTris ='' or exists (select ID from @tblViTri vt2 where vt2.ID= hd.ID_ViTri))
			and (@IDBangGias ='' or exists (select ID from @tblBangGia bg where bg.ID= hd.ID_BangGia))
			 and 
			((select count(Name) from @tblSearch b where     			
				hd.MaHoaDon like '%'+b.Name+'%'
				or hd.NguoiTao like '%'+b.Name+'%'				
				or nv.TenNhanVien like '%'+b.Name+'%'
				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
				or hd.DienGiai like '%'+b.Name+'%'
				or dt.MaDoiTuong like '%'+b.Name+'%'		
				or dt.TenDoiTuong like '%'+b.Name+'%'
				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or dt.DienThoai like '%'+b.Name+'%'		
				or tn.MaPhieuTiepNhan like '%'+b.Name+'%'
				or xe.BienSo like '%'+b.Name+'%'	
				or bh.MaDoiTuong like '%'+b.Name+'%'
				or bh.TenDoiTuong like '%'+b.Name+'%'	
				or bh.TenDoiTuong_KhongDau like '%'+b.Name+'%'				
				)=@count or @count=0)	
			) as c
	WHERE (@BaoHiem= 3 or SuDungBaoHiem = @BaoHiem)
	and exists (select tt.TrangThaiHD from @tblTrangThai tt where c.TrangThaiHD= tt.TrangThaiHD)
	and ( @PhuongThucThanhToan ='' or exists(SELECT Name FROM splitstring(c.PTThanhToan) pt join @tblPhuongThuc pt2 on pt.Name = pt2.PhuongThuc))

	),
	tblDebit as
	(
			select 
				cnLast.ID,
				---- hdDoi co CongNo  < tongtra --> butru = Luyketrahang + conngno
				iif (cnLast.LoaiHoaDonGoc != 6, cnLast.TongTienHDTra,
					iif(cnLast.TongGiaTriTra > cnLast.ConNo, cnLast.TongTienHDTra + cnLast.ConNo,cnLast.TongTienHDTra)) as TongTienHDTra,
					
				iif (cnLast.LoaiHoaDonGoc != 6, cnLast.ConNo,
					iif(cnLast.TongGiaTriTra > cnLast.ConNo, 0, cnLast.ConNo)) as ConNo
						
			from
			(
				select 
					c.ID,
					c.LoaiHoaDonGoc,
					c.TongGiaTriTra,
					iif(c.LoaiHoaDonGoc = 6, iif(c.LuyKeTraHang > 0, 0, abs(c.LuyKeTraHang)), c.LuyKeTraHang) as TongTienHDTra,
					iif(c.ChoThanhToan is null,0, 
						c.TongThanhToan 
							------ neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = 0
							- iif(c.LoaiHoaDonGoc = 6, iif(c.LuyKeTraHang > 0, 0, abs(c.LuyKeTraHang)), c.LuyKeTraHang)
							- c.KhachDaTra - c.BaoHiemDaTra) as ConNo ---- ConNo = TongThanhToan - GtriBuTru
				from data_cte c
			) cnLast 
	),
	
	count_cte
		as (
			select count(dt.ID) as TotalRow,
				CEILING(COUNT(dt.ID) / CAST(@PageSize as float ))  as TotalPage,
				sum(TongTienHang) as SumTongTienHang,
				sum(TongGiamGia) as SumTongGiamGia,
				sum(KhachDaTra) as SumKhachDaTra,
				sum(DaThanhToan) as SumDaThanhToan,
				sum(BaoHiemDaTra) as SumBaoHiemDaTra,
				sum(KhuyeMai_GiamGia) as SumKhuyeMai_GiamGia,
				sum(TongChiPhi) as SumTongChiPhi,
				
				sum(PhaiThanhToan) as SumPhaiThanhToan,
				sum(PhaiThanhToanBaoHiem) as SumPhaiThanhToanBaoHiem,
				sum(TongThanhToan) as SumTongThanhToan,
				sum(TienDoiDiem) as SumTienDoiDiem,
				sum(ThuTuThe) as SumThuTuThe,
				sum(TienDatCoc) as SumTienCoc,
				sum(ThanhTienChuaCK) as SumThanhTienChuaCK,
				sum(GiamGiaCT) as SumGiamGiaCT,
				sum(TienMat) as SumTienMat,
				sum(TienATM) as SumPOS,
				sum(ChuyenKhoan) as SumChuyenKhoan,
				sum(GiaTriSDDV) as TongGiaTriSDDV,
				sum(TongTienThue) as SumTongTienThue,
				sum(ConNo) as SumConNo,

				sum(TongTienThueBaoHiem) as SumTongTienThueBaoHiem,
				sum(TongTienBHDuyet) as SumTongTienBHDuyet,
				sum(KhauTruTheoVu) as SumKhauTruTheoVu,
				sum(GiamTruBoiThuong) as SumGiamTruBoiThuong,
				sum(BHThanhToanTruocThue) as SumBHThanhToanTruocThue				
			from data_cte dt
			left join tblDebit cn on dt.ID= cn.ID
		)
		select dt.*, cte.* , cn.ConNo, cn.TongTienHDTra	
		from data_cte dt
		left join tblDebit cn on dt.ID= cn.ID
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
			when @ColumnSort='MaKhachHang' then MaDoiTuong end ASC,
			case when @SortBy <>'DESC' then ''
			when @ColumnSort='MaKhachHang' then MaDoiTuong end DESC,
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
			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienMat' then TienMat end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienMat' then TienMat end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='ChuyenKhoan' then ChuyenKhoan end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='ChuyenKhoan' then ChuyenKhoan end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienATM' then TienATM end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienATM' then TienATM end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiaTriSDDV' then GiaTriSDDV end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiaTriSDDV' then GiaTriSDDV end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='ThuTuThe' then ThuTuThe end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='ThuTuThe' then ThuTuThe end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TienDatCoc' then TienDatCoc end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TienDatCoc' then TienDatCoc end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='BaoHiemDaTra' then BaoHiemDaTra end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='BaoHiemDaTra' then BaoHiemDaTra end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='PhaiThanhToanBaoHiem' then PhaiThanhToanBaoHiem end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='PhaiThanhToanBaoHiem' then PhaiThanhToanBaoHiem end DESC ,

			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TongTienThueBaoHiem' then TongTienThueBaoHiem end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TongTienThueBaoHiem' then TongTienThueBaoHiem end DESC,			
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='KhauTruTheoVu' then KhauTruTheoVu end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='KhauTruTheoVu' then KhauTruTheoVu end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiamTruBoiThuong' then GiamTruBoiThuong end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiamTruBoiThuong' then GiamTruBoiThuong end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='BHThanhToanTruocThue' then BHThanhToanTruocThue end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='BHThanhToanTruocThue' then BHThanhToanTruocThue end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='TongTienBHDuyet' then TongTienBHDuyet end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='TongTienBHDuyet' then TongTienBHDuyet end DESC,
				case when @SortBy <>'ASC' then 0
			when @ColumnSort='VAT' then TongTienThue end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='VAT' then TongTienThue end DESC
			
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY

	
		drop table #tmpHoaDon


		
End");

			Sql(@"ALTER PROCEDURE [dbo].[TongQuanDoanhThuCongNo]
    @IdChiNhanhs [nvarchar](max),
    @DateFrom [datetime],
    @DateTo [datetime]
AS
BEGIN
    SET NOCOUNT ON;


    	declare @tblDonVi table (ID_DonVi  uniqueidentifier)
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    -- Insert statements for procedure here
    	DECLARE @tblHoaDon TABLE(ID UNIQUEIDENTIFIER, LoaiHoaDon INT, TongThanhToan FLOAT, 		
		PhaiThanhToan FLOAT, 
		TongTienThue FLOAT, ----- TongTienThue (trong DB da bao gom thue KH + thue BH)
		IDHoaDon UNIQUEIDENTIFIER);
    	INSERT INTO @tblHoaDon
    	SELECT ID, LoaiHoaDon, TongThanhToan , PhaiThanhToan, TongTienThue , hd.ID_HoaDon FROM BH_HoaDon hd
    	INNER JOIN @tblDonVi dv ON hd.ID_DonVi = dv.ID_DonVi
    	WHERE hd.LoaiHoaDon IN (1, 4, 6, 7, 19, 25)
    	AND hd.ChoThanhToan = 0
    	AND hd.NgayLapHoaDon BETWEEN @DateFrom AND @DateTo;

		--SELECT * FROM @tblHoaDon
    
    	DECLARE @tblSoQuy TABLE(ID UNIQUEIDENTIFIER, LoaiHoaDon INT, TienMat FLOAT, TienGui FLOAT, TienThu FLOAT, IDHoaDonLienQuan UNIQUEIDENTIFIER);
    	INSERT INTO @tblSoQuy
    	SELECT qhd.ID, qhd.LoaiHoaDon, qhdct.TienMat, qhdct.TienGui, qhdct.TienThu, qhdct.ID_HoaDonLienQuan FROM Quy_HoaDon qhd
    	INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    	INNER JOIN @tblDonVi dv ON qhd.ID_DonVi = dv.ID_DonVi
    	WHERE qhd.NgayLapHoaDon BETWEEN @DateFrom AND @DateTo
		and (qhd.TrangThai = 1 or qhd.TrangThai is null)
		and (qhd.PhieuDieuChinhCongNo != 1 or qhd.PhieuDieuChinhCongNo is null)
    	AND qhd.LoaiHoaDon IN (11, 12);
		--SELECT * FROM @tblSoQuy
    
    	DECLARE @DoanhThuSuaChua FLOAT,
    	@DoanhThuBanHang FLOAT,
    	@PhaiTraKhachHang FLOAT,
    	@PhaiTraNhaCungCap FLOAT,
    	@PhaiThuNhaCungCap FLOAT,
    	@Thu_TienMat FLOAT,
    	@Thu_NganHang FLOAT,
    	@Chi_TienMat FLOAT,
    	@Chi_NganHang FLOAT,
    	@HoaDonDaThu FLOAT,
    	@HoaDonDaChi FLOAT,
		@ThueSuaChua FLOAT,
		@ThueBanHang FLOAT;
    	SELECT @DoanhThuSuaChua =SUM(CASE WHEN hd.LoaiHoaDon = 25 THEN hd.TongThanhToan ELSE 0 END), 
    		@PhaiTraKhachHang = SUM(CASE WHEN hd.LoaiHoaDon = 6 THEN hd.TongThanhToan ELSE 0 END),
    		@DoanhThuBanHang = SUM(CASE WHEN hd.LoaiHoaDon IN (1, 19)
    		THEN
    			hd.PhaiThanhToan
    			ELSE 0
    		END),
    	@PhaiTraNhaCungCap = SUM(CASE WHEN hd.LoaiHoaDon = 4 THEN hd.PhaiThanhToan ELSE 0 END),
    	@PhaiThuNhaCungCap = SUM(CASE WHEN hd.LoaiHoaDon = 7 THEN hd.PhaiThanhToan ELSE 0 END),
		@ThueSuaChua = SUM(CASE WHEN hd.LoaiHoaDon = 25 THEN hd.TongTienThue ELSE 0 END),
		@ThueBanHang = SUM(CASE WHEN hd.LoaiHoaDon IN (1, 19) THEN hd.TongTienThue ELSE 0 END)
    	FROM @tblHoaDon hd;
    
    	SELECT @Thu_TienMat = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.TienMat ELSE 0 END), 
    	@Thu_NganHang = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.TienGui ELSE 0 END),
    	@Chi_TienMat = SUM(CASE WHEN sq.LoaiHoaDon = 12 THEN sq.TienMat ELSE 0 END),
    	@Chi_NganHang = SUM(CASE WHEN sq.LoaiHoaDon = 12 THEN sq.TienGui ELSE 0 END)
    	FROM @tblSoQuy sq;
    
    	SELECT @HoaDonDaThu = SUM(CASE WHEN sq.LoaiHoaDon = 11 THEN sq.ThanhToan ELSE 0 END),  
    	@HoaDonDaChi = SUM(CASE WHEN sq.LoaiHoaDon = 12 AND hd.LoaiHoaDon != 7 THEN sq.ThanhToan ELSE 0 END) FROM @tblHoaDon hd
    	INNER JOIN (SELECT IDHoaDonLienQuan, SUM(TienThu) AS ThanhToan, LoaiHoaDon FROM @tblSoQuy GROUP BY IDHoaDonLienQuan, LoaiHoaDon) sq
    	ON hd.ID = sq.IDHoaDonLienQuan OR (hd.IDHoaDon = sq.IDHoaDonLienQuan AND hd.LoaiHoaDon != 6)
    
    	SET @DoanhThuSuaChua = ISNULL(@DoanhThuSuaChua, 0);
    	SET @DoanhThuBanHang = ISNULL(@DoanhThuBanHang, 0);
    	SET @PhaiTraKhachHang = ISNULL(@PhaiTraKhachHang, 0);
    	SET @PhaiTraNhaCungCap = ISNULL(@PhaiTraNhaCungCap, 0);
    	SET @PhaiThuNhaCungCap = ISNULL(@PhaiThuNhaCungCap, 0);
    	SET @Thu_TienMat = ISNULL(@Thu_TienMat, 0);
    	SET @Thu_NganHang = ISNULL(@Thu_NganHang, 0);
    	SET @Chi_TienMat = ISNULL(@Chi_TienMat, 0);
    	SET @Chi_NganHang = ISNULL(@Chi_NganHang, 0);
    	SET @HoaDonDaThu = ISNULL(@HoaDonDaThu, 0);
    	SET @HoaDonDaChi = ISNULL(@HoaDonDaChi, 0);
		SET @ThueBanHang = ISNULL(@ThueBanHang, 0);
		SET @ThueSuaChua = ISNULL(@ThueSuaChua, 0)
    
	---- DoanhThuSuaChua: khong bao gom thue (sửa lại để trùng với báo cáo --)
    	SELECT @DoanhThuSuaChua - @ThueSuaChua  AS DoanhThuSuaChua, @DoanhThuBanHang - @ThueBanHang AS DoanhThuBanHang,
    	@DoanhThuBanHang + @DoanhThuSuaChua - @ThueSuaChua - @ThueBanHang  AS TongDoanhThu, 
		@DoanhThuBanHang  + @DoanhThuSuaChua + @PhaiThuNhaCungCap - @HoaDonDaThu AS CongNoPhaiThu,
    	@PhaiTraNhaCungCap + @PhaiTraKhachHang - @HoaDonDaChi AS CongNoPhaiTra, 
    	-(@DoanhThuBanHang + @DoanhThuSuaChua + @PhaiThuNhaCungCap - @HoaDonDaThu) + (@PhaiTraNhaCungCap + @PhaiTraKhachHang - @HoaDonDaChi) AS TongCongNo,
    	@Thu_TienMat AS ThuTienMat, @Thu_NganHang AS ThuNganHang, @Thu_TienMat + @Thu_NganHang AS TongTienThu,
    	@Chi_TienMat AS ChiTienMat, @Chi_NganHang AS ChiNganHang, @Chi_TienMat + @Chi_NganHang AS TongTienChi;
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKhachHang_TanSuat]
	@IDChiNhanhs nvarchar(max),	
	@IDNhomKhachs nvarchar(max),
	@LoaiChungTus varchar(20),
	@TrangThaiKhach varchar(10),
	@FromDate datetime,
	@ToDate datetime,
	@NgayGiaoDichFrom datetime,
	@NgayGiaoDichTo datetime,
	@NgayTaoKHFrom datetime,
	@NgayTaoKHTo datetime,
	@DoanhThuTu float,	
	@DoanhThuDen float,
	@SoLanDenFrom int,
	@SoLanDenTo int,
	@TextSearch nvarchar(max),
	@CurrentPage int,
	@PageSize int,
	@ColumnSort varchar(200),
	@TypeSort varchar(20)
AS
BEGIN
	
	SET NOCOUNT ON;


	if @ColumnSort ='' or @ColumnSort is null set @ColumnSort='MaKhachHang'
	if @TypeSort ='' or @TypeSort is null set @TypeSort='desc'
	SET @TypeSort = UPPER(@TypeSort)
	 

	 DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table (ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select * from dbo.splitstring(@IDChiNhanhs)
	
	declare @tblNhomDT table (ID varchar(40))
	insert into @tblNhomDT
	select Name from dbo.splitstring(@IDNhomKhachs);

	declare @tblLoaiHD table (Loai varchar(3))
	insert into @tblLoaiHD
	select Name from dbo.splitstring(@LoaiChungTus);

	
	if @DoanhThuTu is null
		set @DoanhThuTu = -10000000

	if @DoanhThuDen is null
		set @DoanhThuDen = 9999999999
	if @SoLanDenTo is null
		set @SoLanDenTo = 9999999;

	with data_cte
	as(
	select dt.ID as ID_KhachHang, dt.MaDoiTuong as MaKhachHang, dt.TenDoiTuong as TenKhachHang, dt.DienThoai, dt.DiaChi, dt.TenNhomDoiTuongs,
		iif(dt.NgayGiaoDichGanNhat is null,'', FORMAT(dt.NgayGiaoDichGanNhat,'yyyyMMdd HH:mm')) as NgayGiaoDichF,
		dt.NgayGiaoDichGanNhat,
		hd1.SoLanDen,
		GiaTriMua, 
		GiaTriTra,
		DoanhThu
	from DM_DoiTuong dt  
	join (		
			select hd.ID_DoiTuong, 
			count(hd.ID) as SoLanDen, 					
			sum(isnull(hd.GiaTriTra,0)) as GiaTriTra,
			sum(hd.GiaTriMua) as GiaTriMua,
			sum(hd.GiaTriMua - hd.GiaTriTra) as DoanhThu
			from(
				-- doanhthu: khong tinh napthe (chi tinh luc su dung)
				-- vi BC chi tiet theo khachhang: khong lay duoc dichvu/hanghoa khi napthe
				select  hd.ID_DoiTuong, hd.ID,
					iif(hd.LoaiHoaDon= 6, hd.TongTienHang - hd.TongGiamGia,0) as GiaTriTra,
					iif(hd.LoaiHoaDon!= 6, hd.TongTienHang - hd.TongGiamGia - isnull(hd.KhuyeMai_GiamGia,0),0) as GiaTriMua					
				from BH_HoaDon hd
				where hd.ChoThanhToan = 0
				and hd.LoaiHoaDon != 22
				and hd.ID_DoiTuong is not null
				and hd.NgayLapHoaDon between @FromDate and  @ToDate
				and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
				and exists (select Loai from @tblLoaiHD loai where hd.LoaiHoaDon= loai.Loai)
			) hd group by hd.ID_DoiTuong			
	) hd1 on dt.ID = hd1.ID_DoiTuong
    where 
		 exists (select nhom1.Name 
			from dbo.splitstring(iif(dt.IDNhomDoiTuongs is null or dt.IDNhomDoiTuongs ='','00000000-0000-0000-0000-000000000000',dt.IDNhomDoiTuongs)) nhom1 
		 join @tblNhomDT nhom2 on nhom1.Name = nhom2.ID) 
	and
		dt.TheoDoi like @TrangThaiKhach
	and dt.NgayGiaoDichGanNhat between @NgayGiaoDichFrom and  @NgayGiaoDichTo
	and ((dt.NgayTao between @NgayTaoKHFrom and @NgayTaoKHTo) or ( dt.ID ='00000000-0000-0000-0000-000000000000'))
	and hd1.SoLanDen between @SoLanDenFrom and @SoLanDenTo
	and hd1.DoanhThu between @DoanhThuTu and @DoanhThuDen
	and ((select count(Name) from @tblSearchString b where 
    				dt.MaDoiTuong like '%'+b.Name+'%' 
    				or dt.TenDoiTuong like '%'+b.Name+'%' 
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    				or dt.TenDoiTuong_ChuCaiDau like '%' +b.Name +'%' 
    				or dt.DienThoai like '%'+b.Name+'%'
					or dt.DiaChi like '%'+b.Name+'%'
					)=@count or @count=0)
	
	),
	count_cte
	as(
	select count(ID_KhachHang) as TotalRow,
		CEILING(COUNT(ID_KhachHang) / CAST(@PageSize as float )) as TotalPage,
		sum(dt.SoLanDen) as TongSoLanDen,
		sum(GiaTriMua) as TongMua,
		sum(GiaTriTra) as TongTra,
		sum(DoanhThu) as TongDoanhThu
	from data_cte dt
	)
	select *
	from data_cte dt
	cross join count_cte cte
	order by
	case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenNhomDoiTuongs' then TenNhomDoiTuongs end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenNhomDoiTuongs' then TenNhomDoiTuongs end DESC,							
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='MaKhachHang' then MaKhachHang end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='MaKhachHang' then MaKhachHang end DESC,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenKhachHang' then TenKhachHang end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenKhachHang' then TenKhachHang end DESC	,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='DiaChi' then DiaChi end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='DiaChi' then DiaChi end DESC,	
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='DienThoai' then DienThoai end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='DienThoai' then DienThoai end DESC,	
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='SoLanDen' then SoLanDen end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='SoLanDen' then SoLanDen end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='GiaTriMua' then GiaTriMua end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='GiaTriMua' then GiaTriMua end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='GiaTriTra' then GiaTriTra end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='GiaTriTra' then GiaTriTra end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='DoanhThu' then DoanhThu end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='DoanhThu' then DoanhThu end DESC,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='NgayGiaoDichGanNhat' then NgayGiaoDichF end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='NgayGiaoDichGanNhat' then NgayGiaoDichF end DESC		
	OFFSET (@CurrentPage* @PageSize) ROWS
	FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER FUNCTION [dbo].[BaoDuong_GetTongGiaTriNhac]
(
	@LanBaoDuong int,
	@ID_HangHoa uniqueidentifier
)
RETURNS float
AS
BEGIN
	DECLARE @TongGiaTri float

	declare @tmp table(ID uniqueidentifier, ID_HangHoa uniqueidentifier, LanBaoDuong int, GiaTri float, LoaiGiaTri int, LapDinhKy int)
			insert into @tmp
			select *
			from DM_HangHoa_BaoDuongChiTiet where ID_HangHoa= @ID_HangHoa
		
			declare @tblGiaTri table (GiaTri float)	
			declare @flag int= @LanBaoDuong;
			
			while @flag!=0
			begin
				insert into @tblGiaTri 
				select 
					case LoaiGiaTri 
						when 4 then GiaTri
						when 3 then 365* GiaTri
						when 2 then 30 * GiaTri
						when 1 then GiaTri
						end
				from @tmp where LanBaoDuong= @flag
				set @flag = @flag - 1
			end
			
		select @TongGiaTri= sum(GiaTri)
		from @tblGiaTri

	RETURN @TongGiaTri

END
");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_HoaDonNhapHang]
    @TextSearch [nvarchar](max),
    @LoaiHoaDon varchar(50), ---- dùng chung cho nhập hàng + trả hàng nhập + nhập kho nội bộ
    @IDChiNhanhs [nvarchar](max),
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

		declare @tblLoaiHD table (Loai int)
    	insert into @tblLoaiHD
    	select Name from dbo.splitstring(@LoaiHoaDon)
    
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch)


    
    	--;with data_cte
    	--as (
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

			Sql(@"ALTER PROCEDURE [dbo].[SP_BaoCaoKhuyenMai]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    	Select @count =  (Select count(*) from @tblSearchString);
       
    	select * from 
    	(
    		select km.MaKhuyenMai, km.TenKhuyenMai,km.HinhThuc, hd.ID_DonVi, hd.MaHoaDon,hd.LoaiHoaDon, hd.NgayLapHoaDon, hd.TongTienHang, hd.NguoiTao, 
    			 qd.MaHangHoa, hh.TenHangHoa,
				 case when km.HinhThuc = 12 or km.HinhThuc = 13 then ct.SoLuong else 0 end as SoLuong, 
    			 case km.HinhThuc
					 when 11 then hd.KhuyeMai_GiamGia
					 when 12 then ct.SoLuong * ct.DonGia -- tanghang
					 when 13 then  ct.SoLuong * ct.TienChietKhau -- gghang
					 when 14 then hd.DiemKhuyenMai
					 else 0 end as GiaTriKM, 
    			ISNULL(dt.MaDoiTuong, '') as MaDoiTuong,ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
				ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau, 
    			'' as TenDonViTinh,'' as LaHangHoa, '1' as TheoDoi, '0' as Xoa,
    			null as TenHangHoa_KhongDau, null as TenHangHoa_KyTuDau,
    			nv.MaNhanVien, nv.TenNhanVien,TenNhanVienChuCaiDau,TenNhanVienKhongDau, '00000000-0000-0000-0000-000000000000' as ID_NhomHang,
    			case HinhThuc
    				when 11 then N'Hóa đơn - Giảm hóa đơn'
    				when 12 then N'Hóa đơn -Tặng hàng'
    				when 13 then N'Hóa đơn - Giảm giá hàng'
    				when 14 then N'Hóa đơn - Tặng điểm'
    				end as sHinhThuc
    		from BH_HoaDon hd
    		join DM_KhuyenMai km on hd.ID_KhuyenMai = km.ID
			left join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon 
				and hd.ID_KhuyenMai = ct.ID_KhuyenMai and ct.TangKem = '1' -- used to hoadon - gghang, hoadon - tanghang
    		left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
    		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
			left join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    		left join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    		INNER JOIN (select * from splitstring(@ID_ChiNhanh)) lstID_DonVi ON lstID_DonVi.Name = hd.ID_DonVi			
    		where hd.ID_KhuyenMai is not null
    		and hd.NgayLapHoaDon >= @timeStart AND hd.NgayLapHoaDon < @timeEnd
    		and hd.ChoThanhToan = 0
    
    		union all
    
    		select MaKhuyenMai,TenKhuyenMai,HinhThuc,kmhh.ID_DonVi, MaHoaDon,LoaiHoaDon, NgayLapHoaDon, TongTienHang,kmhh.NguoiTao, MaHangHoa,TenHangHoa,SoLuong, GiaTriKM,
    			ISNULL(dt.MaDoiTuong, '') as MaDoiTuong, ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong, ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau, 
    			TenDonViTinh, LaHangHoa,kmhh.TheoDoi,kmhh.Xoa,TenHangHoa_KhongDau,TenHangHoa_KyTuDau,
    			MaNhanVien,TenNhanVien,TenNhanVienChuCaiDau,TenNhanVienKhongDau, ID_NhomHang,
    			case HinhThuc
    				when 21 then N'Hàng hóa - Giảm giá hàng'
    				when 22 then N'Hàng hóa - Tặng hàng'
    				when 23 then N'Hàng hóa - Tặng điểm'
    				when 24 then N'Hàng hóa - Giá bán theo số lượng mua'
    				end as sHinhThuc
    		from (			  
					select 		
						hd.ID,
						hd.MaHoaDon, 
						hd.NgayLapHoaDon,
						hd.LoaiHoaDon,
						hd.ID_DoiTuong,
						hd.ID_NhanVien,
						hd.ID_DonVi, 
						hd.NguoiTao,
						ct.SoLuong - isnull(SoLuongTra,0) as SoLuong,
						ctkm.ID_TangKem, 
						ctkm.TangKem,
						hd.TongTienHang,
						km.HinhThuc, 
						km.MaKhuyenMai, 
						km.TenKhuyenMai,
						case km.HinhThuc
    						when 21 then ctkm.SoLuong *  ctkm.TienChietKhau
    						when 22 then (ct.SoLuong - isnull(SoLuongTra,0)) * ctkm.DonGia
							when 23 then 0
    						when 24 then ctkm.SoLuong * ctkm.TienChietKhau    				
    						end as GiaTriKM,
						hh.TenHangHoa,
						hh.TenHangHoa_KhongDau, hh.TenHangHoa_KyTuDau,hh.LaHangHoa,hh.ID_NhomHang,
						iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
						qd.MaHangHoa,
						qd.TenDonViTinh, 
						hh.TheoDoi, 
						qd.Xoa
    				from BH_HoaDon_ChiTiet ct
    				join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    				join DM_KhuyenMai km on ct.ID_KhuyenMai = km.ID
    				join BH_HoaDon_ChiTiet ctkm on ct.ID_DonViQuiDoi = ctkm.ID_TangKem and ct.ID_HoaDon = ctkm.ID_HoaDon and (ctkm.ID_TangKem is not null or ctkm.Tangkem ='0')
					join DonViQuiDoi qd on ctkm.ID_DonViQuiDoi = qd.ID
    				join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
					left join(
						---- trahang khuyenmai ----
						select 
							ct.ID_ChiTietGoiDV,						
							sum(ct.SoLuong) as SoLuongTra
						from BH_HoaDon hdt
						join BH_HoaDon_ChiTiet ct on hdt.ID= ct.ID_HoaDon
						where hdt.LoaiHoaDon= 6 
						and hdt.ChoThanhToan='0'
						and ct.TangKem ='1' ----- chi get hang tangkem 
						group by ct.ID_ChiTietGoiDV
						) traKM on ctkm.ID = traKM.ID_ChiTietGoiDV				
					where ct.ID_KhuyenMai is not null
    				and hd.NgayLapHoaDon >= @timeStart AND hd.NgayLapHoaDon < @timeEnd
    				and hd.ChoThanhToan = 0 and hd.LoaiHoaDon !=6
					and exists (select * from splitstring(@ID_ChiNhanh) cn where hd.ID_DonVi = cn.Name)			
				) kmhh
    			left join DM_DoiTuong dt on kmhh.ID_DoiTuong = dt.ID
    			left join NS_NhanVien nv on kmhh.ID_NhanVien = nv.ID
    			where (kmhh.ID_TangKem is not null or TangKem ='1')
				and kmhh.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))
    	) tbl
    	inner join (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)) allnhh ON tbl.ID_NhomHang = allnhh.ID		
    	where  tbl.TheoDoi like @TheoDoi    		
		and tbl.Xoa like @TrangThai
    		and tbl.LoaiHoaDon in (select * from splitstring(@LoaiChungTu))
    		AND ((select count(Name) from @tblSearchString b where 
    		tbl.MaHoaDon like '%'+b.Name+'%' 
    	OR tbl.MaHoaDon like '%'+b.Name+'%' 
    	or tbl.MaHangHoa like '%'+b.Name+'%' 
    	or tbl.TenHangHoa like '%'+b.Name+'%'
    	or tbl.TenHangHoa_KhongDau like '%' +b.Name +'%' 
    		or tbl.TenHangHoa_KyTuDau like '%' +b.Name +'%'
    		or tbl.MaNhanVien like '%'+b.Name+'%'
    		or tbl.TenNhanVien like '%'+b.Name+'%'
    		or tbl.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    		or tbl.TenNhanVienKhongDau like '%'+b.Name+'%'
    		or tbl.TenDonViTinh like '%'+b.Name+'%'
    		or tbl.MaKhuyenMai like '%'+b.Name+'%'
    		or tbl.TenKhuyenMai like '%'+b.Name+'%'
    		or tbl.MaDoiTuong like '%'+b.Name+'%'
    		or tbl.TenDoiTuong like '%'+b.Name+'%'
    		or tbl.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    		or tbl.sHinhThuc like '%'+b.Name+'%'
    		or dbo.FUNC_ConvertStringToUnsign(sHinhThuc) like '%'+b.Name+'%'
    		)=@count or @count=0)
    	order by NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListPromotion]
	@IDChiNhanhs varchar(max),
    @TextSearch nvarchar(max),
    @TypePromotion nvarchar(4),-- %%.all, 0.hoadon, 1.hanghoa
    @StatusActive nvarchar(4),-- %%.all, 0.notactive, 1.active, 
    @Expired nvarchar(4), -- %%.all, 1. hethan, 2: conhan 
    @CurrentPage int,
    @PageSize int
AS
BEGIN
	
	SET NOCOUNT ON;
	declare @today datetime = format(getdate(),'yyyy-MM-dd HH:mm')
	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	with data_cte
		as(
	select *
	from
		(SELECT 
			km.ID,
			--ad.ID_DonVi,
			km.MaKhuyenMai,
			km.TenKhuyenMai,
			km.GhiChu,
			Case when km.TrangThai = 1 then N'Kích hoạt' else N'Chưa áp dụng' end as TrangThai,
			case km.HinhThuc
				when 11 then N'Hóa đơn - Giảm giá hóa đơn'
				when 12 then N'Hóa đơn - Tặng hàng'
				when 13 then N'Hóa đơn - Giảm giá hàng'
				when 14 then N'Hóa đơn - Tặng Điểm'
				when 21 then N'Hàng hóa - Mua hàng giảm giá hàng'
				when 22 then N'Hàng hóa - Mua hàng tặng hàng'
				when 23 then N'Hàng hóa - Mua hàng tặng điểm'
				when 24 then N'Hàng hóa - Mua hàng giảm giá theo số lượng mua'
			end as HinhThuc,
			km.LoaiKhuyenMai,
			km.HinhThuc as KieuHinhThuc,
			km.ThoiGianBatDau,
			km.ThoiGianKetThuc,
			Case when km.NgayApDung = '' then '' else N'Ngày ' + Replace(km.NgayApDung, '_', N', Ngày ') end as NgayApDung,
			Case when km.ThangApDung = '' then '' else N'Tháng ' + Replace(km.ThangApDung, '_', N', Tháng ') end as ThangApDung,
			Replace(Case when km.ThuApDung = '' then '' else N'Thứ ' + Replace(km.ThuApDung, '_', N', Thứ ') end, N'Thứ 8',N'Chủ nhật') as ThuApDung,
			Case when km.GioApDung = '' then '' else Replace(km.GioApDung, '_', N', ') end as GioApDung,
			Case when km.ApDungNgaySinhNhat = 1 then N'Áp dụng vào ngày sinh nhật khách hàng' when km.ApDungNgaySinhNhat = 2 then N'Áp dụng vào tuần sinh nhật khách hàng'
			when km.ApDungNgaySinhNhat = 3 then N'Áp dụng vào tháng sinh nhật khách hàng' else '' end as ApDungNgaySinhNhat,
			km.ApDungNgaySinhNhat as ValueApDungSN,
			km.TatCaDoiTuong,
			km.TatCaDonVi,
			km.TatCaNhanVien,
			km.NguoiTao,
			km.NgayTao,
			case when format(ThoiGianBatDau,'yyyy-MM-dd HH:mm') > @today OR format(ThoiGianKetThuc,'yyyy-MM-dd HH:mm') < @today then '1' else '2' end as Expired					
		FROM DM_KhuyenMai km
		where km.TrangThai like @StatusActive
		and LoaiKhuyenMai like @TypePromotion
		and (km.TatCaDonVi = '1' 
		---- check exist donvi apdung
			or exists(select * from DM_KhuyenMai_ApDung ad where km.ID = ad.ID_KhuyenMai 
			and exists (select Name from dbo.splitstring(@IDChiNhanhs) where ad.ID_DonVi = Name) )
			)
		AND ((select count(Name) from @tblSearchString b where 
    		km.MaKhuyenMai like '%'+b.Name+'%' 
    		or km.TenKhuyenMai like '%'+b.Name+'%' 
    		)=@count or @count=0)
	) a
	where Expired like @Expired
	),
	count_cte
		as (
			select count(*) as TotalRow,
				CEILING(COUNT(*) / CAST(@PageSize as float ))  as TotalPage				
			from data_cte
		)
		select dt.*, cte.*
		from data_cte dt
		cross join count_cte cte
		order by dt.NgayTao desc
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetInforHoaDon_ByID]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    
    declare @IDHDGoc uniqueidentifier, @ID_DoiTuong uniqueidentifier, @ID_BaoHiem uniqueidentifier
    select @IDHDGoc = ID_HoaDon,  @ID_DoiTuong = ID_DoiTuong, @ID_BaoHiem = ID_BaoHiem from BH_HoaDon where ID = @ID_HoaDon

   
    select 
    			hd.ID,
    			hd.LoaiHoaDon,
    			hd.MaHoaDon,
    			hd.NgayLapHoaDon,
    			hd.ID_PhieuTiepNhan, 
    			hd.TongTienHang,
    			hd.ChoThanhToan,
				hd.TongChiPhi,
    			ISNULL(hd.TongGiamGia,0) + ISNULL(hd.KhuyeMai_GiamGia, 0) as TongGiamGia,
    			CAST(ISNULL(hd.PhaiThanhToan,0) as float)  as PhaiThanhToan,
    			CAST(ISNULL(KhachDaTra,0) as float) as KhachDaTra,	
    			isnull(soquy.BaoHiemDaTra,0) as BaoHiemDaTra,
    
    			CAST(ISNULL(TienDoiDiem,0) as float) as TienDoiDiem,	
    			CAST(ISNULL(ThuTuThe,0) as float) as ThuTuThe,	
    			isnull(soquy.TienMat,0) as TienMat,
    			isnull(soquy.TienATM,0) as TienATM,
    			isnull(soquy.ChuyenKhoan,0) as ChuyenKhoan,
    
    			dt.MaDoiTuong,
    			bh.TenDoiTuong as TenBaoHiem,
    			bh.MaDoiTuong as MaBaoHiem,
    
    			ISNULL(dt.TenDoiTuong,N'Khách lẻ')  as TenDoiTuong,
    			ISNULL(bg.TenGiaBan,N'Bảng giá chung') as TenBangGia,
    			ISNULL(nv.TenNhanVien,N'')  as TenNhanVien,
    			ISNULL(dv.TenDonVi,N'')  as TenDonVi,    
    			case when hd.NgayApDungGoiDV is null then '' else  convert(varchar(14), hd.NgayApDungGoiDV ,103) end  as NgayApDungGoiDV,
    			case when hd.HanSuDungGoiDV is null then '' else  convert(varchar(14), hd.HanSuDungGoiDV ,103) end as HanSuDungGoiDV,
    			hd.NguoiTao as NguoiTaoHD,
    			hd.DienGiai,
    			hd.ID_DonVi,
    			hd.TongTienThue,
    			isnull(hd.TongTienBHDuyet,0) as TongTienBHDuyet, 
    			isnull(hd.PTThueHoaDon,0) as PTThueHoaDon, 
    			isnull(hd.PTThueBaoHiem,0) as PTThueBaoHiem, 
    			isnull(hd.TongTienThueBaoHiem,0) as TongTienThueBaoHiem, 
    			isnull(hd.KhauTruTheoVu,0) as KhauTruTheoVu, 
    
    			isnull(hd.PTGiamTruBoiThuong,0) as PTGiamTruBoiThuong, 
    			isnull(hd.GiamTruBoiThuong,0) as GiamTruBoiThuong, 
    			isnull(hd.BHThanhToanTruocThue,0) as BHThanhToanTruocThue, 
    			isnull(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem, 

				cpVC.ID_NhaCungCap,
				isnull(cpVC.TenDoiTuong,'') as TenNCCVanChuyen, 
    			isnull(cpVC.MaDoiTuong,'') as MaNCCVanChuyen, 

    		-- get avoid error at variable at class BH_HoaDonDTO
    			NEWID() as ID_DonViQuiDoi,
    			case when hd.ChoThanhToan is null then N'Đã hủy'
    			else 
    				case hd.LoaiHoaDon
    					when 3 then 
    						case when hd.YeuCau ='1' then case when hd.ID_PhieuTiepNhan is null then  N'Phiếu tạm' else
    						case when hd.ChoThanhToan='0' then N'Đã duyệt' else N'Chờ duyệt' end end
    						else 
    							case when hd.YeuCau ='2' then  N'Đang xử lý'
    							else 
    								case when hd.YeuCau ='3' then N'Hoàn thành'
    								else  N'Đã hủy' end
    							end end
    						else N'Hoàn thành'
    						end end as TrangThai		   														
    	from BH_HoaDon hd
    	left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
    	left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    	left join DM_DonVi dv on hd.ID_DonVi= dv.ID
    	left join DM_GiaBan bg on hd.ID_BangGia= bg.ID
    	left join DM_DoiTuong bh on hd.ID_BaoHiem= bh.ID
		left join
		(
			select cp.ID_HoaDon, cp.ID_NhaCungCap, ncc.MaDoiTuong, ncc.TenDoiTuong
			from BH_HoaDon_ChiPhi cp 
			join DM_DoiTuong ncc on cp.ID_NhaCungCap= ncc.ID
			where cp.ID_HoaDon_ChiTiet is null
		) cpVC on hd.ID= cpVC.ID_HoaDon
    	left join 
    		(
    				select 
    					tblThuChi.ID,
    					sum(TienMat) as TienMat,
    					sum(TienATM) as TienATM,
    					sum(ChuyenKhoan) as ChuyenKhoan,
    					sum(TienDoiDiem) as TienDoiDiem,
    					sum(ThuTuThe) as ThuTuThe,				
    					sum(KhachDaTra) as KhachDaTra,
    					sum(BaoHiemDaTra) as BaoHiemDaTra
    				from
    					(
    					-- get TongThu from HDDatHang: chi get hdXuly first
    					select 
    							ID,
    							TienMat, TienATM,ChuyenKhoan,
    							TienDoiDiem, ThuTuThe, 				
    							TienThu as KhachDaTra,	
    							0 as BaoHiemDaTra,
    							TienDatCoc
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
    												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
    												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
    												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
    												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
    												iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
    												iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu,
    												iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc
    											from Quy_HoaDon_ChiTiet qct
    											join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
    											join BH_HoaDon hdd on hdd.ID= qct.ID_HoaDonLienQuan
    											join BH_HoaDon hd on hd.ID_HoaDon= hdd.ID
    											where hdd.LoaiHoaDon = '3' 	
    											and hd.ChoThanhToan = 0
    											and (qhd.TrangThai= 1 Or qhd.TrangThai is null)
    											and hdd.ID= @IDHDGoc											
    								) d group by d.ID,d.NgayLapHoaDon,ID_HoaDonLienQuan						
    						) thuDH
    						where isFirst= 1
    
    						union all
    					---- khach datra
    					select qct.ID_HoaDonLienQuan,		
    						sum(iif(qct.HinhThucThanhToan= 1,iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu),0))  as TienMat,
    						sum(iif(qct.HinhThucThanhToan= 2,iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu),0))  as TienATM,
    						sum(iif(qct.HinhThucThanhToan= 3,iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu),0))  as ChuyenKhoan,
    						sum(iif(qct.HinhThucThanhToan= 5,iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu),0))  as TienDoiDiem,
    						sum(iif(qct.HinhThucThanhToan= 4,iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu),0))  as ThuTuThe,					
    						sum(iif(qhd.LoaiHoaDon=12 ,- qct.TienThu, qct.TienThu)) as KhachDaTra,
    						0 as BaoHiemDaTra,
    						0 as TienDatCoc
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					where qhd.TrangThai= 1
    					and qct.ID_DoiTuong= @ID_DoiTuong and qct.ID_HoaDonLienQuan = @ID_HoaDon					
    					group by qct.ID_HoaDonLienQuan
    
    
    					union all
    					----- baohiem datra
    					select qct.ID_HoaDonLienQuan, 
    						0 as TienMat,
    						0 as TienATM,
    						0 as ChuyenKhoan,
    						0 as TienDoiDiem,
    						0 as ThuTuThe,
    									
    						0 as KhachDaTra, 
    						sum(qct.TienThu) as BaoHiemDaTra,
    						0 as TienDatCoc
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					where qhd.TrangThai= 1 
    					and qct.ID_DoiTuong= @ID_BaoHiem and qct.ID_HoaDonLienQuan = @ID_HoaDon							
    					group by qct.ID_HoaDonLienQuan
    				)tblThuChi group by tblThuChi.ID
    			) soquy on hd.ID = soquy.ID		
    	where hd.ID like @ID_HoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetQuyChiTiet_byIDQuy]
    @ID [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
		
	---- get allhoadon lienquan by idSoQuy
	select distinct ID_HoaDonLienQuan , ID_DoiTuong
	into #tblHoaDon
	from Quy_HoaDon_ChiTiet qct
	where qct.ID_HoaDon = @ID	
	group by qct.ID_HoaDonLienQuan, qct.ID_DoiTuong


	---- get phieuthu/chi cua chinhno
		select 
			qct.ID_HoaDonLienQuan,
			qct.ID_DoiTuong,
			sum(iif(qhd.LoaiHoaDon =11, qct.TienThu, - qct.TienThu)) as ThuChinhNo
	into #thuChinhNo
	 from Quy_HoaDon qhd
    join Quy_HoaDon_ChiTiet qct on qhd.ID = qct.ID_HoaDon
	where qhd.ID = @ID
	and (qhd.TrangThai ='1' or qhd.TrangThai is null)
	group by qct.ID_HoaDonLienQuan,qct.ID_DoiTuong

	---- get allPhieuThu of hoadon --> tinh giatri DaChiTruoc ---
	select 
		qct.ID_HoaDonLienQuan,
		qct.ID_DoiTuong,
		sum(iif(qhd.LoaiHoaDon =11, qct.TienThu, - qct.TienThu)) as KhachDaTra
	into #thuChiAll
	from Quy_HoaDon qhd
    join Quy_HoaDon_ChiTiet qct on qhd.ID = qct.ID_HoaDon
	where exists
		(select * from #tblHoaDon hdIn where hdIn.ID_HoaDonLienQuan = qct.ID_HoaDonLienQuan and qct.ID_DoiTuong = hdIn.ID_DoiTuong)
	and (qhd.TrangThai ='1' or qhd.TrangThai is null)
	group by qct.ID_HoaDonLienQuan,qct.ID_DoiTuong

	----- get BuTruTraHang ----
	select 
		tblLuyKe.ID,
		iif(tblLuyKe.LoaiHoaDonGoc = 6, iif(tblLuyKe.LuyKeTraHang > 0, 0, abs(tblLuyKe.LuyKeTraHang)), tblLuyKe.LuyKeTraHang) as TongTienHDTra
	into #tblLuyKe
	from
	(
		select 
			hdGocTra.ID,
			hdGocTra.ID_HoaDonGoc,
			hdGocTra.LoaiHoaDonGoc ,			
			isnull(iif(LoaiHoaDonGoc = 3 or ID_HoaDon is null,								
									case when TongGiaTriTra > PhaiThanhToan then PhaiThanhToan else TongGiaTriTra end,											
										(select dbo.BuTruTraHang_HDDoi(ID_HoaDon,NgayLapHoaDon,ID_HoaDonGoc, LoaiHoaDonGoc))				
					),0) as LuyKeTraHang
		from
		(
				select hd.ID,
					hd.PhaiThanhToan,
					hd.NgayLapHoaDon,
					hd.ID_HoaDon,
					hdgoc.LoaiHoaDon as LoaiHoaDonGoc,
					hdgoc.ID_HoaDon as ID_HoaDonGoc,
					ISNULL(allTra.TongGtriTra,0) as TongGiaTriTra		
				from BH_HoaDon hd
				left join BH_HoaDon hdgoc on hd.ID_HoaDon= hdgoc.ID
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
				) allTra on allTra.ID_HoaDon = hd.ID
				where exists (select * from #tblHoaDon hdIn where hd.ID = hdIn.ID_HoaDonLienQuan)
		) hdGocTra
	) tblLuyKe

	---- get chiphi dichvu NCC
	select 
			cp.ID_HoaDon,
			cp.ID_NhaCungCap,
			sum(cp.ThanhTien) as PhaiThanhToan
		into #tblChiPhi
		from BH_HoaDon_ChiPhi cp
		where exists (select * from #tblHoaDon hd where cp.ID_HoaDon = hd.ID_HoaDonLienQuan)	
		group by cp.ID_HoaDon, cp.ID_NhaCungCap
		

    select qhd.id, qct.ID_HoaDon, qhd.MaHoaDon, qhd.NguoiTao, qhd.LoaiHoaDon, qhd.TongTienThu, qhd.ID_NhanVien, qhd.NoiDungThu,
		 qhd.ID_DonVi,	qhd.HachToanKinhDoanh, qhd.PhieuDieuChinhCongNo,qhd.NguoiSua, isnull(qhd.TrangThai, '1') as TrangThai,
    	  iif(qct.HinhThucThanhToan=1, qct.TienThu,0) as TienMat, 
		  iif(qct.HinhThucThanhToan=2 or qct.HinhThucThanhToan=3 , qct.TienThu,0) as TienGui, 
			qct.TienThu, qct.DiemThanhToan, qct.ID_TaiKhoanNganHang, qct.ID_KhoanThuChi, 
		   qhd.NgayLapHoaDon as NgayLapPhieuThu,
    	   qct.ID_DoiTuong,
		   qct.ID_BangLuongChiTiet,
    	   qct.ID_HoaDonLienQuan,
    	   qct.ID_NhanVien as ID_NhanVienCT, -- thu/chi cho NV nao
    	   qct.HinhThucThanhToan,
    	   cast(iif(qhd.LoaiHoaDon = 11,'1','0') as bit) as LaKhoanThu,
    	   iif(qct.LoaiThanhToan = 1,1,0) as LaTienCoc,
    	   isnull(hd.MaHoaDon,N'Thu thêm') as MaHoaDonHD,    	  
		   nv.TenNhanVien,
		   dt.MaDoiTuong as MaDoiTuong, 
		   dt.TenDoiTuong as NguoiNopTien, 	
		   dt.DienThoai as SoDienThoai,
    	   case dt.LoaiDoiTuong
    		when 1 then 1
    		when 2 then 2
    		when 3 then 3
    		else 0 end as LoaiDoiTuong,
    		iif(qhd.TrangThai ='0', N'Đã hủy', N'Đã thanh toán') as GhiChu,	  
    	   iif( hd.NgayLapHoaDon is null, qhd.NgayLapHoaDon, hd.NgayLapHoaDon) as NgayLapHoaDon,
    	   case qct.HinhThucThanhToan
    			when 1 then  N'Tiền mặt'
    			when 2 then  N'POS'
    			when 3 then  N'Chuyển khoản'
    			when 4 then  N'Thu từ thẻ'
    			when 5 then  N'Đổi điểm'
    			when 6 then  N'Thu từ cọc'
    		end as PhuongThuc,
			ktc.NoiDungThuChi,
			iif(ktc.LaKhoanThu is null,  IIF(qhd.LoaiHoaDon=11,'1','0'), ktc.LaKhoanThu) as LaKhoanThu,
			iif(tk.TaiKhoanPOS ='1',tk.TenChuThe,'') as TenTaiKhoanPOS,
			iif(tk.TaiKhoanPos ='0',tk.TenChuThe,'') as TenTaiKhoanNOTPOS,	
			isnull(hd.LoaiHoaDon,0) as LoaiHoaDonHD,
			isnull(iif(dt.LoaiDoiTuong =3, hd.TongTienThueBaoHiem,iif(hd.TongThueKhachHang >0, hd.TongThueKhachHang, hd.TongTienThue)),0) as TongTienThue,
			isnull(luyke.TongTienHDTra,0) as TongTienHDTra,		
			 dt.LoaiDoiTuong,
			
			case dt.LoaiDoiTuong
				when 3 then hd.PhaiThanhToanBaoHiem
				else
					---- neu ben vanchuyen la ben# -----
					case when hd.LoaiHoaDon = 4 then iif(cp.ID_NhaCungCap is not null, 
							iif(qct.ID_DoiTuong = cp.ID_NhaCungCap, hd.TongChiPhi, hd.PhaiThanhToan - hd.TongChiPhi), hd.PhaiThanhToan)
					else hd.PhaiThanhToan end
				end as TongThanhToanHD, ---- giữ nguyên gtrị hóa đơn (không bù trừ trả hàng) --> tính hoa hồng NV theo doanh số					
			abs(isnull(cn.KhachDaTra,0)) - abs(isnull(thisSQ.ThuChinhNo,0)) as DaThuTruoc,
			tk.TaiKhoanPOS,
			nh.TenNganHang,
			nh.ChiPhiThanhToan,
			nh.MacDinh,
			nh.TheoPhanTram,
			nh.ThuPhiThanhToan
    from Quy_HoaDon qhd
    join Quy_HoaDon_ChiTiet qct on qhd.ID = qct.ID_HoaDon
    left join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID
	left join #tblLuyKe luyke on hd.ID = luyke.ID
	left join #thuChiAll cn on hd.ID = cn.ID_HoaDonLienQuan
	left join #tblChiPhi cp on hd.ID= cp.ID_HoaDon and qct.ID_HoaDonLienQuan = cp.ID_HoaDon
    left join DM_DoiTuong dt on qct.ID_DoiTuong= dt.ID
	left join NS_NhanVien nv on qhd.ID_NhanVien= nv.ID
	left join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi = ktc.ID
	left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang = tk.ID
	left join DM_NganHang nh on tk.ID_NganHang = nh.ID
	left join #thuChinhNo thisSQ  on thisSQ.ID_HoaDonLienQuan = qct.ID_HoaDonLienQuan and thisSQ.ID_DoiTuong = qct.ID_DoiTuong
    where qhd.ID= @ID
	order by hd.NgayLapHoaDon
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
    
    		set @sql1= 'declare @count int = 0'
    
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
    					sum(cp.ThanhTien) as PhiDichVu
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

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetHDDebit_ofKhachHang]
    @ID_DoiTuong [nvarchar](max),
    @ID_DonVi [nvarchar](max),
	@LoaiDoiTuong int
AS
BEGIN
	if @ID_DonVi='00000000-0000-0000-0000-000000000000'
		begin
			set @ID_DonVi = (select CAST(ID as varchar(40)) + ',' as  [text()] from DM_DonVi  where TrangThai is null or TrangThai='1' for xml path(''))	
			set @ID_DonVi= left(@ID_DonVi, LEN(@ID_DonVi) -1) -- remove last comma ,
		end
		
			select 
				a.ID, a.MaHoaDon, a.NgayLapHoaDon,	a.LoaiHoaDon,
				a.TongThanhToan,
				a.TongTienThue,
				TinhChietKhauTheo,
				ID_NhaCungCap,			
				iif(@LoaiDoiTuong=3, a.PhaiThanhToanBaoHiem,
					---- neu vanchuyen la ben # --> khong tinh chiphi vc nay cho ben cungcap
					case when ID_NhaCungCap is not null then iif(@ID_DoiTuong = ID_NhaCungCap, TongChiPhi, PhaiThanhToan - TongChiPhi)
					else PhaiThanhToan end ) as PhaiThanhToan
			into #hdKH_BH
			from
			(				
				select 
					hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
					hd.TongTienThue,
					hd.TongThanhToan,
    				ISNULL(hd.PhaiThanhToan,0)  as PhaiThanhToan,
					ISNULL(hd.PhaiThanhToanBaoHiem,0)  as PhaiThanhToanBaoHiem,
    				ISNULL(tblNV.TinhChietKhauTheo,1) as TinhChietKhauTheo,
					hd.ID_NhaCungCap,
					ISNULL(hd.TongChiPhi,0) as TongChiPhi -- used check when thanhtoan nhaphang
				from
					(
					select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
						hd.TongTienThue,
						hd.TongThanhToan,
    					ISNULL(hd.PhaiThanhToan,0)  as PhaiThanhToan,
						ISNULL(hd.PhaiThanhToanBaoHiem,0)  as PhaiThanhToanBaoHiem,
						hd.TongChiPhi,
						cpVC.ID_NhaCungCap
    				from BH_HoaDon hd   	
					left join BH_HoaDon_ChiPhi cpVC on hd.ID= cpVC.ID_HoaDon and cpVC.ID_HoaDon_ChiTiet is null --- only get cpvc of phieu nhaphang
					where  hd.LoaiHoaDon in (1,19,4,22, 25,13)
					and exists (select Name from dbo.splitstring(@ID_DonVi) where Name= hd.ID_DonVi)
					and iif(@LoaiDoiTuong=3, hd.ID_BaoHiem, hd.ID_DoiTuong) like @ID_DoiTuong		   				
    				and hd.ChoThanhToan='0' 
				) hd
    			left join 
    				(select ID_HoaDon, min(TinhChietKhauTheo) as TinhChietKhauTheo
    				from BH_NhanVienThucHien nvth
    				where nvth.ID_HoaDon is not null
    				group by ID_HoaDon
					) tblNV on hd.ID = tblNV.ID_HoaDon
    			
			) a

		----- get list ID of HD
		declare @tblID TblID
		insert into @tblID
		select ID from #hdKH_BH
		
		declare @LoaiHDTra int = iif(@LoaiDoiTuong=2,7,6)
		------ get congno of list HD
		declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, BuTruTraHang float, KhachDaTra float)
		insert into @tblCongNo
		exec HD_GetBuTruTraHang @tblID,@LoaiHDTra

		select *
		from
		(
			select 
					hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
					hd.TongThanhToan,
					hd.TongTienThue,
					TinhChietKhauTheo,
					iif(@LoaiDoiTuong=3,0,isnull(cn.BuTruTraHang, 0)) as TongTienHDTra,
					hd.PhaiThanhToan				
			from #hdKH_BH hd
			left join @tblCongNo cn on hd.ID= cn.ID

			union all

			---- phaithanhtoan of NCC (chiphi)
			select 
				cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon,hd.LoaiHoaDon,
				sum(cp.ThanhTien) as TongThanhToan,
				0 as TongTienThue,
				0 as TinhChietKhauTheo,
				0 as BuTruTraHang,
				sum(cp.ThanhTien) as PhaiThanhToan
			from BH_HoaDon_ChiPhi cp
			join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
			where hd.ChoThanhToan= 0
			and cp.ID_NhaCungCap= @ID_DoiTuong
			group by cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon,	hd.LoaiHoaDon
		)tblView order by NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetHoaDonAndSoQuy_FromIDDoiTuong]
    @ID_DoiTuong [nvarchar](max),
    @ID_DonVi [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;

declare @tblChiNhanh table (ID uniqueidentifier)
insert into @tblChiNhanh
select name from dbo.splitstring(@ID_DonVi)


	DECLARE @tblHoaDon TABLE(ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, LoaiHoaDon INT, GiaTri FLOAT);
	DECLARE @LoaiDoiTuong INT;
	SELECT @LoaiDoiTuong = LoaiDoiTuong FROM DM_DoiTuong WHERE ID = @ID_DoiTuong;
	IF(@LoaiDoiTuong = 3)
	BEGIN
		INSERT INTO @tblHoaDon
    	select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
			hd.PhaiThanhToanBaoHiem as GiaTri
    	from BH_HoaDon hd
    	where hd.ID_BaoHiem like @ID_DoiTuong and hd.ID_DonVi in (select ID from @tblChiNhanh)
    	and hd.LoaiHoaDon not in (3,23) -- dieu chinh the: khong lien quan cong no
		and hd.ChoThanhToan ='0'
	END
	ELSE
	BEGIN
		INSERT INTO @tblHoaDon
		select *
		from
		(
		select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
			case hd.LoaiHoaDon
				when 4 then iif(cp.ID_NhaCungCap is not null,  ISNULL(hd.TongThanhToan,0) - isnull(hd.TongChiPhi,0), ISNULL(hd.TongThanhToan,0))
				when 6 then - ISNULL(hd.TongThanhToan,0)
				when 7 then - ISNULL(hd.TongThanhToan,0)
			else
    			ISNULL(hd.PhaiThanhToan,0)
    		end as GiaTri
    	from BH_HoaDon hd
		join @tblChiNhanh cn on hd.ID_DonVi= cn.ID
		left join BH_HoaDon_ChiPhi cp on hd.ID= cp.ID_HoaDon and cp.ID_HoaDon_ChiTiet is null ---- chiphi vc ben thu3
    	where hd.ID_DoiTuong like @ID_DoiTuong 
    	and hd.LoaiHoaDon not in (3,23,31) 
		and hd.ChoThanhToan ='0'


		union all
		---- chiphi dichvu
		select 
			cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon, max(iif(cp.ID_HoaDon_ChiTiet is null, 124, 125)) as LoaiHoaDon,
			sum(cp.ThanhTien) as GiaTri			
		from BH_HoaDon_ChiPhi cp
		join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
		join @tblChiNhanh cn on hd.ID_DonVi= cn.ID
		where hd.ChoThanhToan= 0
		and cp.ID_NhaCungCap= @ID_DoiTuong
		group by cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon,hd.LoaiHoaDon
		)a
	END

	---select * from @tblHoaDon
		
		SELECT *, 0 as LoaiThanhToan
		FROM @tblHoaDon
    	union
    	-- get list Quy_HD (không lấy Quy_HoaDon thu từ datcoc)
		select * from
		(
    		select qhd.ID, qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.LoaiHoaDon ,
			case when dt.LoaiDoiTuong = 1 OR dt.LoaiDoiTuong = 3 then
				case when qhd.LoaiHoaDon= 11 then -sum(qct.TienThu) else iif (max(LoaiThanhToan)=4, -sum(qct.TienThu),sum(qct.TienThu)) end
			else 
    			case when qhd.LoaiHoaDon = 11 then sum(qct.TienThu) else -sum(qct.TienThu) end
    		end as GiaTri,
			iif(qhd.PhieuDieuChinhCongNo='1',2, max(isnull(qct.LoaiThanhToan,0))) as LoaiThanhToan --- 1.coc, 2.dieuchinhcongno, 3.khong butru congno, 4.tralai sodu TGT			
    		from Quy_HoaDon qhd	
    		join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
    		join DM_DoiTuong dt on qct.ID_DoiTuong= dt.ID
			left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID		
    		where qct.ID_DoiTuong like @ID_DoiTuong
			and exists (select ID from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID) ---- chi lay phieuthu/chi cua chi nhanh dang chon
			and (qct.HinhThucThanhToan !=6 or qct.HinhThucThanhToan is null)
	
			and (qct.LoaiThanhToan is null or qct.LoaiThanhToan !=3) ---- khong get phieuthu/chi khong lienquan congno
			and (qhd.TrangThai is null or qhd.TrangThai='1') -- van phai lay phieu thu tu the --> trừ cong no KH
			group by qhd.ID, qhd.MaHoaDon, qhd.NgayLapHoaDon, qhd.LoaiHoaDon,dt.LoaiDoiTuong,qhd.PhieuDieuChinhCongNo
		) a where a.GiaTri != 0 -- khong lay phieudieuchinh diem

	
END
");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateGiaVon_WhenEditCTHD]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier],
    @NgayLapHDMin [datetime]
AS
BEGIN
    SET NOCOUNT ON;	
	

    		declare @tblCTHD ChiTietHoaDonEdit
    		INSERT INTO @tblCTHD
    		SELECT 
    			qd.ID_HangHoa, ct.ID_LoHang, ct.ID_DonViQuiDoi, qd.TyLeChuyenDoi
    		FROM BH_HoaDon_ChiTiet ct
    		INNER JOIN BH_HoaDon hd ON hd.ID = ct.ID_HoaDon			
    		INNER JOIN DonViQuiDoi qd ON qd.ID = ct.ID_DonViQuiDoi			
    		INNER JOIN DM_HangHoa hh on hh.ID = qd.ID_HangHoa    		
    		WHERE hd.ID = @IDHoaDonInput AND hh.LaHangHoa = 1
    		GROUP BY qd.ID_HangHoa,ct.ID_DonViQuiDoi,qd.TyLeChuyenDoi, ct.ID_LoHang, hd.ID_DonVi, hd.ID_CheckIn, hd.YeuCau, hd.NgaySua, hd.NgayLapHoaDon;	
    
    		-- get cthd can update GiaVon
    		DECLARE @cthd_NeedUpGiaVon TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonBan UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, ID_ChiTietHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, SoThuThu INT, SoLuong FLOAT, DonGia FLOAT, 
			TongTienHang FLOAT, TongChiPhi FLOAT,
    	ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT, TonKho FLOAT, GiaVon FLOAT, GiaVonNhan FLOAT, ID_HangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, IDDonViQuiDoi UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER, 
    	ID_ChiNhanhThemMoi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX));
    	INSERT INTO @cthd_NeedUpGiaVon  	
		select hd.ID as IDHoaDon,
			hd.ID_HoaDon as IDHoaDonGoc, 
			hd.MaHoaDon, 
			hd.LoaiHoaDon,
			ct.ID as ID_ChiTietHoaDon, 
    		CASE WHEN hd.YeuCau = '4' AND @IDChiNhanh = hd.ID_CheckIn THEN hd.NgaySua ELSE hd.NgayLapHoaDon END AS NgayLapHoaDon, 				    			    				    							    			
    		ct.SoThuTu,
			iif(ct.ChatLieu='5',0,ct.SoLuong) as SoLuong, 
			ct.DonGia, 
			hd.TongTienHang, 
			isnull(hd.TongChiPhi,0) as TongChiPhi,
			ct.TienChietKhau,
			ct.ThanhTien, 
			hd.TongGiamGia, 
			qd.TyLeChuyenDoi,
			[dbo].[FUNC_TonLuyKeTruocThoiGian](@IDChiNhanh, hh.ID, ct.ID_LoHang, 
			CASE WHEN hd.YeuCau = '4' AND @IDChiNhanh = hd.ID_CheckIn THEN hd.NgaySua ELSE hd.NgayLapHoaDon END) as TonKho, 	 	    	
    		ct.GiaVon / qd.TyLeChuyenDoi as GiaVon, 
    		ct.GiaVon_NhanChuyenHang / qd.TyLeChuyenDoi as GiaVonNhan,
    		hh.ID as ID_HangHoa, 
			hh.LaHangHoa, 
			qd.ID as IDDonViQuiDoi, 
			ct.ID_LoHang, 
			ct.ID_ChiTietDinhLuong, 
    		@IDChiNhanh as IDChiNhanh,
			hd.ID_CheckIn,
			hd.YeuCau 
    	FROM BH_HoaDon_ChiTiet ct 
    	INNER JOIN DonViQuiDoi qd ON ct.ID_DonViQuiDoi = qd.ID   	
    	INNER JOIN DM_HangHoa hh on hh.ID = qd.ID_HangHoa   	
    	INNER JOIN BH_HoaDon hd  ON ct.ID_HoaDon = hd.ID   
    	WHERE hd.ChoThanhToan = 0    		
			and hd.LoaiHoaDon NOT IN (3, 19, 25,29)
			---- dont join ctEdit because douple row (only check exists ID_QuiDoi, ID_Lo)
			and exists (select ID_HangHoa from @tblCTHD ctNew where ctNew.ID_HangHoa = qd.ID_HangHoa  and (ct.ID_LoHang = ctNew.ID_LoHang OR ctNew.ID_LoHang IS NULL))
    			AND
    			((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon >= @NgayLapHDMin
    				and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    				or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and hd.NgaySua >= @NgayLapHDMin))
    
    			---- BEGIN get giavon dauky by id_quidoi =========
    		DECLARE @TinhGiaVonTrungBinh BIT;
    		SELECT @TinhGiaVonTrungBinh = GiaVonTrungBinh FROM HT_CauHinhPhanMem WHERE ID_DonVi = @IDChiNhanh;
    		IF(@TinhGiaVonTrungBinh IS NOT NULL AND @TinhGiaVonTrungBinh = 1)
    		BEGIN
    		
    		DECLARE @ChiTietHoaDonGiaVon TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonBan UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, ID_ChiTietHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, SoThuThu INT, SoLuong FLOAT, DonGia FLOAT,
			TongTienHang FLOAT, TongChiPhi float,
    		ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT, TonKho FLOAT, GiaVon FLOAT, GiaVonNhan FLOAT, ID_HangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, IDDonViQuiDoi UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER,  
    		ID_ChiNhanhThemMoi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX));
    		INSERT INTO @ChiTietHoaDonGiaVon
    		select
    				hd.ID, hd.ID_HoaDon, hd.MaHoaDon, hd.LoaiHoaDon, hdct.ID, hd.NgayLapHoaDon, hdct.SoThuTu,
					iif(hdct.ChatLieu='5',0, hdct.SoLuong) as SoLuong, 
					hdct.DonGia, 
					hd.TongTienHang, 
					isnull(hd.TongChiPhi,0) as TongChiPhi,
    				iif(hdct.ChatLieu='5',0,hdct.TienChietKhau) as TienChietKhau, 
					hdct.ThanhTien, hd.TongGiamGia, 
    				dvqd.TyLeChuyenDoi, 0, hdct.GiaVon / dvqd.TyLeChuyenDoi as GiaVon, 
    			hdct.GiaVon_NhanChuyenHang / dvqd.TyLeChuyenDoi  as GiaVonNhan, 
    			hh.ID, hh.LaHangHoa, hdct.ID_DonViQuiDoi, hdct.ID_LoHang, hdct.ID_ChiTietDinhLuong, 
    				@IDChiNhanh, hd.ID_CheckIn, hd.YeuCau
			FROM BH_HoaDon hd
    		INNER JOIN BH_HoaDon_ChiTiet hdct ON hd.ID = hdct.ID_HoaDon    	
    		INNER JOIN DonViQuiDoi dvqd ON hdct.ID_DonViQuiDoi = dvqd.ID    		
    		INNER JOIN DM_HangHoa hh on hh.ID = dvqd.ID_HangHoa    		
    		INNER JOIN @tblCTHD cthdthemmoi ON cthdthemmoi.ID_HangHoa = hh.ID    		
    		WHERE hd.ChoThanhToan = 0 
				and hd.LoaiHoaDon NOT IN (3, 19, 25,29)
    				AND (hdct.ID_LoHang = cthdthemmoi.ID_LoHang OR cthdthemmoi.ID_LoHang IS NULL) 
    				AND
    				((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon < @NgayLapHDMin and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    					or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and hd.NgaySua < @NgayLapHDMin))
    	   			

		DECLARE @ChiTietHoaDonGiaVon1 TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonBan UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, ID_ChiTietHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, SoThuThu INT, SoLuong FLOAT, DonGia FLOAT,
			TongTienHang FLOAT, TongChiPhi FLOAT,
    		ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT, TonKho FLOAT, GiaVon FLOAT, GiaVonNhan FLOAT, ID_HangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, IDDonViQuiDoi UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER,  
    		ID_ChiNhanhThemMoi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX), RN INT);
			
			INSERT INTO @ChiTietHoaDonGiaVon1
			SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    					FROM @ChiTietHoaDonGiaVon) AS cthdGiaVon1 WHERE cthdGiaVon1.RN = 1;
			---- === END giavon dauky ========= ---

    		------ union 2 table: ctEdit + ctNeddUpdateGV: partition by idhang,idlohang order by NgayLapHoaDon
    		DECLARE @BangUpdateGiaVon TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonBan UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, ID_ChiTietHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, SoThuThu INT, SoLuong FLOAT, DonGia FLOAT,
			TongTienHang FLOAT, TongChiPhi FLOAT,
    		ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT, TonKho FLOAT, GiaVon FLOAT, GiaVonNhan FLOAT, ID_HangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, IDDonViQuiDoi UNIQUEIDENTIFIER, ID_LoHang UNIQUEIDENTIFIER, ID_ChiTietDinhLuong UNIQUEIDENTIFIER,
    		ID_ChiNhanhThemMoi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX), RN INT);
    		INSERT INTO @BangUpdateGiaVon
    		SELECT *, 
    				ROW_NUMBER() OVER (PARTITION BY tableUpdateGiaVon.ID_HangHoa, tableUpdateGiaVon.ID_LoHang ORDER BY tableUpdateGiaVon.NgayLapHoaDon) AS RN 
    			FROM
    			(
					SELECT * 
    					FROM @cthd_NeedUpGiaVon
    				UNION ALL
    					SELECT 
    						gvDauKy.IDHoaDon, gvDauKy.IDHoaDonBan, gvDauKy.MaHoaDon, gvDauKy.LoaiHoaDon, gvDauKy.ID_ChiTietHoaDon, gvDauKy.NgayLapHoaDon,
    						gvDauKy.SoThuThu, gvDauKy.SoLuong, gvDauKy.DonGia, gvDauKy.TongTienHang, gvDauKy.TongChiPhi,
    						gvDauKy.ChietKhau, gvDauKy.ThanhTien, gvDauKy.TongGiamGia, gvDauKy.TyLeChuyenDoi, gvDauKy.TonKho, 
    						CASE WHEN gvDauKy.GiaVon IS NULL THEN 0 ELSE gvDauKy.GiaVon END AS GiaVon, 											
    						CASE WHEN gvDauKy.GiaVonNhan IS NULL THEN 0 ELSE gvDauKy.GiaVonNhan END AS GiaVonNhan,								
    						cthd1.ID_HangHoa, gvDauKy.LaHangHoa, gvDauKy.IDDonViQuiDoi, cthd1.ID_LoHang , gvDauKy.ID_ChiTietDinhLuong,
    					@IDChiNhanh as ID_ChiNhanh, gvDauKy.ID_CheckIn, gvDauKy.YeuCau 
    					FROM @tblCTHD cthd1
    					LEFT JOIN @ChiTietHoaDonGiaVon1 AS gvDauKy ON cthd1.ID_HangHoa = gvDauKy.ID_HangHoa   					   				
    					AND (cthd1.ID_LoHang = gvDauKy.ID_LoHang OR gvDauKy.ID_LoHang IS NULL)				
			 ) tableUpdateGiaVon
    
    			-- caculator again GiaVon by ID_HangHoa
    			DECLARE @TableTrungGianUpDate TABLE(IDHoaDon UNIQUEIDENTIFIER,IDHangHoa UNIQUEIDENTIFIER, IDLoHang UNIQUEIDENTIFIER, GiaVonNhapHang FLOAT, GiaVonNhanHang FLOAT)
    			INSERT INTO @TableTrungGianUpDate
    			SELECT 
    				IDHoaDon, ID_HangHoa, ID_LoHang, 
					sum(GiaVon) as GiaVonNhapHang,
					sum(GiaVonNhan) as GiaVonNhanHang
    			FROM @BangUpdateGiaVon
    			WHERE IDHoaDon = @IDHoaDonInput and RN= 1
    			GROUP BY ID_HangHoa, ID_LoHang, IDHoaDon,LoaiHoaDon
    			
    			
    
    			--DECLARE @RNCheck INT;
    			--SELECT @RNCheck = MAX(RN) FROM @BangUpdateGiaVon GROUP BY ID_HangHoa, ID_LoHang
    			--IF(@RNCheck = 1)
    			--BEGIN
    			--	UPDATE @BangUpdateGiaVon SET RN = 2
    			--END

			update gv set gv.RN = 2
				 from @BangUpdateGiaVon gv
				 join 
						(select  MAX(RN) as RN, ID_HangHoa, ID_LoHang FROM @BangUpdateGiaVon GROUP BY ID_HangHoa, ID_LoHang 
				) tbl on gv.ID_HangHoa = tbl.ID_HangHoa and (gv.ID_LoHang = tbl.ID_LoHang or gv.ID_LoHang is null and tbl.ID_LoHang is null)
				 where tbl.RN= 1
    
    			---- update GiaVon, GiaVonNhan to @BangUpdateGiaVon if Loai =(4,10,13), else keep old
    		UPDATE bhctup 
    			SET bhctup.GiaVon = 
    			CASE WHEN bhctup.LoaiHoaDon in (4,13) THEN giavontbup.GiaVonNhapHang	    						
    			ELSE bhctup.GiaVon END,    		
    			bhctup.GiaVonNhan = 
    				CASE WHEN bhctup.LoaiHoaDon = 10 AND bhctup.YeuCau = '4' AND bhctup.ID_CheckIn = ID_ChiNhanhThemMoi THEN giavontbup.GiaVonNhanHang   		    			
    			ELSE bhctup.GiaVonNhan END  		
    			FROM @BangUpdateGiaVon bhctup
    			JOIN @TableTrungGianUpDate giavontbup on bhctup.IDHoaDon =giavontbup.IDHoaDon and bhctup.ID_HangHoa = giavontbup.IDHangHoa and (bhctup.ID_LoHang = giavontbup.IDLoHang or giavontbup.IDLoHang is null)
    		WHERE bhctup.IDHoaDon = @IDHoaDonInput AND bhctup.RN = 1;
    			--END tính giá vốn trung bình cho lần nhập hàng và chuyển hàng đầu tiền
    
    		DECLARE @GiaVonCapNhat TABLE (IDHoaDon UNIQUEIDENTIFIER, IDHoaDonBan UNIQUEIDENTIFIER, IDHoaDonCu UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), LoaiHoaDon INT, IDChiTietHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, SoLuong FLOAT, DonGia FLOAT, 
			TongTienHang FLOAT, TongChiPhi FLOAT,
    		ChietKhau FLOAT, ThanhTien FLOAT, TongGiamGia FLOAT, TyLeChuyenDoi FLOAT, TonKho FLOAT, GiaVon FLOAT, GiaVonNhan FLOAT, GiaVonCu FLOAT, IDHangHoa UNIQUEIDENTIFIER, LaHangHoa BIT, IDDonViQuiDoi UNIQUEIDENTIFIER, IDLoHang UNIQUEIDENTIFIER, IDChiTietDinhLuong UNIQUEIDENTIFIER,
    		IDChiNhanhThemMoi UNIQUEIDENTIFIER, IDCheckIn UNIQUEIDENTIFIER, YeuCau NVARCHAR(MAX), RN INT);
    		INSERT INTO @GiaVonCapNhat
    		SELECT 
    				tableUpdate.IDHoaDon, tableUpdate.IDHoaDonBan, tableGiaVon.IDHoaDon, tableUpdate.MaHoaDon, tableUpdate.LoaiHoaDon, tableUpdate.ID_ChiTietHoaDon,tableUpdate.NgayLapHoaDon, tableUpdate.SoLuong, tableUpdate.DonGia,
    			tableUpdate.TongTienHang, tableUpdate.TongChiPhi,
				tableUpdate.ChietKhau, tableUpdate.ThanhTien, tableUpdate.TongGiamGia, tableUpdate.TyLeChuyenDoi, tableUpdate.TonKho, tableUpdate.GiaVon, tableUpdate.GiaVonNhan, tableGiaVon.GiaVon, tableUpdate.ID_HangHoa, tableUpdate.LaHangHoa,
    			tableUpdate.IDDonViQuiDoi, tableUpdate.ID_LoHang, tableUpdate.ID_ChiTietDinhLuong, tableUpdate.ID_ChiNhanhThemMoi, tableUpdate.ID_CheckIn, tableUpdate.YeuCau, tableUpdate.RN 
    			FROM @BangUpdateGiaVon tableUpdate
    		LEFT JOIN (SELECT (CASE WHEN ID_CheckIn = ID_ChiNhanhThemMoi THEN GiaVonNhan ELSE GiaVon END) AS GiaVon, ID_HangHoa, IDHoaDon, ID_LoHang, RN + 1 AS RN 
    						FROM @BangUpdateGiaVon) AS tableGiaVon
    		ON tableUpdate.ID_HangHoa = tableGiaVon.ID_HangHoa AND tableUpdate.RN = tableGiaVon.RN 
    			AND (tableUpdate.ID_LoHang = tableGiaVon.ID_LoHang OR tableUpdate.ID_LoHang IS NULL);
    
    			--select * from @GiaVonCapNhat
    			
    		DECLARE @IDHoaDon UNIQUEIDENTIFIER;
    		DECLARE @IDHoaDonBan UNIQUEIDENTIFIER;
    		DECLARE @IDHoaDonCu UNIQUEIDENTIFIER;
    		DECLARE @MaHoaDon NVARCHAR(MAX);
    		DECLARE @LoaiHoaDon INT;
    		DECLARE @IDChiTietHoaDon UNIQUEIDENTIFIER;
    		DECLARE @SoLuong FLOAT;
    		DECLARE @DonGia FLOAT;
    		DECLARE @TongTienHang float, @TongChiPhi float;
    		DECLARE @ChietKhau FLOAT;
    		DECLARE @ThanhTien FLOAT;
    		DECLARE @TongGiamGia FLOAT;
    		DECLARE @TyLeChuyenDoi FLOAT;
    		DECLARE @TonKho FLOAT;
    		DECLARE @GiaVonCu FLOAT;
    		DECLARE @IDHangHoa UNIQUEIDENTIFIER;
    		DECLARE @IDDonViQuiDoi UNIQUEIDENTIFIER;
    		DECLARE @IDLoHang UNIQUEIDENTIFIER;
    		DECLARE @IDChiNhanhThemMoi UNIQUEIDENTIFIER;
    		DECLARE @IDCheckIn UNIQUEIDENTIFIER;
    		DECLARE @YeuCau NVARCHAR(MAX);
    		DECLARE @RN INT;
    			DECLARE @GiaVon FLOAT;
    			DECLARE @GiaVonNhan FLOAT;
    		DECLARE @GiaVonMoi FLOAT;
    		DECLARE @GiaVonCuUpdate FLOAT;
    		DECLARE @IDHangHoaUpdate UNIQUEIDENTIFIER;
    		DECLARE @IDLoHangUpdate UNIQUEIDENTIFIER;
    			DECLARE @GiaVonHoaDonBan FLOAT;
    		DECLARE @TongTienHangDemo FLOAT;
    		DECLARE @SoLuongDemo FLOAT;
    			DECLARE @ThanhTienDemo FLOAT;
    			DECLARE @ChietKhauDemo FLOAT;
    
    		DECLARE CS_GiaVon CURSOR SCROLL LOCAL FOR 
    			SELECT IDHoaDon, IDHoaDonBan, IDHoaDonCu, MaHoaDon, LoaiHoaDon, IDChiTietHoaDon, SoLuong, DonGia, TongTienHang, TongChiPhi,
				ChietKhau,ThanhTien, TongGiamGia, TyLeChuyenDoi, TonKho,
    			GiaVonCu, IDHangHoa, IDDonViQuiDoi, IDLoHang, IDChiNhanhThemMoi, IDCheckIn, YeuCau, RN, GiaVon, GiaVonNhan 
    			FROM @GiaVonCapNhat WHERE RN > 1 and LaHangHoa = 1 ORDER BY IDHangHoa, RN
    		OPEN CS_GiaVon
    		FETCH FIRST FROM CS_GiaVon 
    			INTO @IDHoaDon, @IDHoaDonBan, @IDHoaDonCu, @MaHoaDon, @LoaiHoaDon, @IDChiTietHoaDon, @SoLuong, @DonGia, 
				@TongTienHang, @TongChiPhi, @ChietKhau,@ThanhTien, @TongGiamGia, @TyLeChuyenDoi, @TonKho,
    			@GiaVonCu, @IDHangHoa, @IDDonViQuiDoi, @IDLoHang, @IDChiNhanhThemMoi, @IDCheckIn, @YeuCau, @RN, @GiaVon, @GiaVonNhan
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
    			iF(@IDHangHoaUpdate = @IDHangHoa AND (@IDLoHangUpdate = @IDLoHang OR @IDLoHang IS NULL))
    			BEGIN
    				SET @GiaVonCu = @GiaVonCuUpdate;
    			END
    			ELSE
    			BEGIN
    				SET @IDHangHoaUpdate = @IDHangHoa;
    				SET @IDLoHangUpdate = @IDLoHang;
    					SET @GiaVonCuUpdate = @GiaVonCu;
    			END
    				IF(@GiaVonCu IS NOT NULL)
    				BEGIN
    				IF(@LoaiHoaDon in (4,13))
    				BEGIN
    					SELECT @TongTienHangDemo = SUM(bhctdm.SoLuong * (bhctdm.DonGia -  bhctdm.ChietKhau)), @SoLuongDemo = SUM(bhctdm.SoLuong * dvqddm.TyLeChuyenDoi) 
    					FROM @GiaVonCapNhat bhctdm
    					left Join DonViQuiDoi dvqddm on bhctdm.IDDonViQuiDoi = dvqddm.ID
    					WHERE bhctdm.IDHoaDon = @IDHoaDon AND dvqddm.ID_HangHoa = @IDHangHoa AND (bhctdm.IDLoHang = @IDLoHang or @IDLoHang is null)
    					GROUP BY dvqddm.ID_HangHoa, bhctdm.IDLoHang
        					
    					IF(@SoLuongDemo + @TonKho > 0 AND @TonKho > 0)
    					BEGIN
    						IF(@TongTienHang != 0)
    						BEGIN
								------ giavon: tinh sau khi tru giam gia hoadon + phi ship
    							SET	@GiaVonMoi = ((@GiaVonCu * @TonKho) + (@TongTienHangDemo* (1-(@TongGiamGia/@TongTienHang)))
								+ (@TongTienHangDemo* @TongChiPhi/@TongTienHang))/(@SoLuongDemo + @TonKho);
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
    
    						--select @GiaVonMoi
    				END
    				ELSE IF (@LoaiHoaDon = 7)
    				BEGIN
    					--select @IDHoaDon, @MaHoaDon, @TongTienHangDemo, @SoLuongDemo, @TonKho
    						
    					SELECT @TongTienHangDemo = 
							SUM(bhctdm.SoLuong * bhctdm.DonGia * ( 1- bhctdm.TongGiamGia/iif(bhctdm.TongTienHang=0,1,bhctdm.TongTienHang))) ,
							@SoLuongDemo = SUM(bhctdm.SoLuong * dvqddm.TyLeChuyenDoi) 
    					FROM @GiaVonCapNhat bhctdm
    					left Join DonViQuiDoi dvqddm on bhctdm.IDDonViQuiDoi = dvqddm.ID
    					WHERE bhctdm.IDHoaDon = @IDHoaDon AND dvqddm.ID_HangHoa = @IDHangHoa AND (bhctdm.IDLoHang = @IDLoHang or @IDLoHang is null)
    					GROUP BY dvqddm.ID_HangHoa, bhctdm.IDLoHang
    					IF(@TonKho - @SoLuongDemo > 0)
    					BEGIN
    						SET	@GiaVonMoi = ((@GiaVonCu * @TonKho) - @TongTienHangDemo)/(@TonKho - @SoLuongDemo);
    					END
    					ELSE
    					BEGIN
    						SET @GiaVonMoi = @GiaVonCu;
    					END
    						--select @GiaVonMoi
    				END
    				ELSE IF(@LoaiHoaDon = 10)
    				BEGIN
    					SELECT @TongTienHangDemo = SUM(bhctdm.ChietKhau * bhctdm.DonGia), @SoLuongDemo = SUM(bhctdm.ChietKhau * dvqddm.TyLeChuyenDoi) 
    					FROM @GiaVonCapNhat bhctdm
    					left Join DonViQuiDoi dvqddm on bhctdm.IDDonViQuiDoi = dvqddm.ID
    					WHERE bhctdm.IDHoaDon = @IDHoaDon AND dvqddm.ID_HangHoa = @IDHangHoa AND (bhctdm.IDLoHang = @IDLoHang or @IDLoHang is null)
    					GROUP BY dvqddm.ID_HangHoa, bhctdm.IDLoHang
    
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
    				END
    				ELSE IF (@LoaiHoaDon = 6)
    				BEGIN
    					SELECT @SoLuongDemo = SUM(bhctdm.SoLuong * dvqddm.TyLeChuyenDoi) 
    					FROM @GiaVonCapNhat bhctdm
    					left Join DonViQuiDoi dvqddm on bhctdm.IDDonViQuiDoi = dvqddm.ID
    					WHERE bhctdm.IDHoaDon = @IDHoaDon AND dvqddm.ID_HangHoa = @IDHangHoa AND (bhctdm.IDLoHang = @IDLoHang or @IDLoHang is null)
    					GROUP BY dvqddm.ID_HangHoa, bhctdm.IDLoHang
    					IF(@IDHoaDonBan IS NOT NULL)
    					BEGIN
    						SET @GiaVonHoaDonBan = -1;
    						SELECT @GiaVonHoaDonBan = GiaVon FROM @GiaVonCapNhat WHERE IDHoaDon = @IDHoaDonBan AND IDDonViQuiDoi = @IDDonViQuiDoi AND (IDLoHang = @IDLoHang OR @IDLoHang IS NULL);
    						
    						IF(@GiaVonHoaDonBan = -1)
    						BEGIN
    							
    							SELECT @GiaVonHoaDonBan = GiaVon / @TyLeChuyenDoi FROM BH_HoaDon_ChiTiet WHERE ID_HoaDon = @IDHoaDonBan AND ID_DonViQuiDoi = @IDDonViQuiDoi AND (ID_LoHang = @IDLoHang OR @IDLoHang IS NULL);
    						
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
    						SET @GiaVonMoi = @GiaVonCu;
    					END
    				END
    				ELSE IF(@LoaiHoaDon = 18)
    					BEGIN
    						SELECT @GiaVonMoi = GiaVon / @TyLeChuyenDoi FROM BH_HoaDon_ChiTiet WHERE ID_HoaDon = @IDHoaDon AND ID_DonViQuiDoi = @IDDonViQuiDoi AND (ID_LoHang = @IDLoHang OR @IDLoHang IS NULL);
    					END
    					ELSE
    				BEGIN
    					SET @GiaVonMoi = @GiaVonCu;
    				END
    				END
    				ELSE
    				BEGIN
    					IF(@IDCheckIn = @IDChiNhanhThemMoi)
    					BEGIN
    						SET @GiaVonMoi = @GiaVonNhan
    					END
    					ELSE
    					BEGIN
    						SET @GiaVonMoi = @GiaVon
    					END
    				END
    
    			IF(@IDHoaDon = @IDHoaDonCu)
    			BEGIN
    				SET @GiaVonMoi = @GiaVonCuUpdate;	
    			END
    			ELSE
    			BEGIN
    				SET @GiaVonCuUpdate = @GiaVonMoi;
    			END
    			IF(@LoaiHoaDon = 10 AND @YeuCau = '4' AND @IDCheckIn = @IDChiNhanhThemMoi)
    			BEGIN
    				UPDATE @GiaVonCapNhat SET GiaVonNhan = @GiaVonMoi WHERE IDHangHoa = @IDHangHoa AND (IDLoHang = @IDLoHang OR @IDLoHang IS NULL) AND RN = @RN;
    				UPDATE @GiaVonCapNhat SET GiaVonCu = @GiaVonMoi WHERE IDHangHoa = @IDHangHoa AND (IDLoHang = @IDLoHang OR @IDLoHang IS NULL) AND RN = @RN +1;
    			END
    			ELSE
    			BEGIN
    				UPDATE @GiaVonCapNhat SET GiaVon = @GiaVonMoi WHERE IDHangHoa = @IDHangHoa AND (IDLoHang = @IDLoHang OR @IDLoHang IS NULL) AND RN = @RN;
    				UPDATE @GiaVonCapNhat SET GiaVonCu = @GiaVonMoi WHERE IDHangHoa = @IDHangHoa AND (IDLoHang = @IDLoHang OR @IDLoHang IS NULL) AND RN = @RN +1;
    			END
    			FETCH NEXT FROM CS_GiaVon INTO @IDHoaDon, @IDHoaDonBan, @IDHoaDonCu, @MaHoaDon, @LoaiHoaDon, @IDChiTietHoaDon, @SoLuong, @DonGia, 
				@TongTienHang, @TongChiPhi, @ChietKhau, @ThanhTien, @TongGiamGia, @TyLeChuyenDoi, @TonKho,
    			@GiaVonCu, @IDHangHoa, @IDDonViQuiDoi, @IDLoHang, @IDChiNhanhThemMoi, @IDCheckIn, @YeuCau, @RN, @GiaVon, @GiaVonNhan
    		END
    		CLOSE CS_GiaVon
    		DEALLOCATE CS_GiaVon		
			
			update gv set GiaVonCu = isnull(GiaVonCu,0)
			from @GiaVonCapNhat gv 
    

    		---- =========Update BH_HoaDon_ChiTiet
			begin try
    		UPDATE hoadonchitiet1
    		SET hoadonchitiet1.GiaVon = _giavoncapnhat1.GiaVon * _giavoncapnhat1.TyLeChuyenDoi,
    			hoadonchitiet1.GiaVon_NhanChuyenHang = _giavoncapnhat1.GiaVonNhan * _giavoncapnhat1.TyLeChuyenDoi
    		FROM BH_HoaDon_ChiTiet AS hoadonchitiet1
    		INNER JOIN @GiaVonCapNhat AS _giavoncapnhat1 ON hoadonchitiet1.ID = _giavoncapnhat1.IDChiTietHoaDon   		
    		WHERE _giavoncapnhat1.LoaiHoaDon != 8 AND _giavoncapnhat1.LoaiHoaDon != 18 AND _giavoncapnhat1.LoaiHoaDon != 9 AND _giavoncapnhat1.RN > 1;
    
    		---- update GiaVon to phieu KiemKe
    			UPDATE hoadonchitiet4
    		SET hoadonchitiet4.GiaVon = _giavoncapnhat4.GiaVon * _giavoncapnhat4.TyLeChuyenDoi, 
    			hoadonchitiet4.ThanhToan = _giavoncapnhat4.GiaVon * _giavoncapnhat4.TyLeChuyenDoi *hoadonchitiet4.SoLuong
    		FROM BH_HoaDon_ChiTiet AS hoadonchitiet4
    		INNER JOIN @GiaVonCapNhat AS _giavoncapnhat4 ON hoadonchitiet4.ID = _giavoncapnhat4.IDChiTietHoaDon    		
    		WHERE _giavoncapnhat4.LoaiHoaDon = 9 AND _giavoncapnhat4.RN > 1;
    
    			-- update GiaVon to phieu XuatKho
    		UPDATE hoadonchitiet2
    		SET hoadonchitiet2.GiaVon = _giavoncapnhat2.GiaVon * _giavoncapnhat2.TyLeChuyenDoi, 
				--hoadonchitiet2.DonGia = _giavoncapnhat2.GiaVon * hoadonchitiet2.SoLuong* _giavoncapnhat2.TyLeChuyenDoi,
    			hoadonchitiet2.ThanhTien = _giavoncapnhat2.GiaVon * hoadonchitiet2.SoLuong* _giavoncapnhat2.TyLeChuyenDoi
    		FROM BH_HoaDon_ChiTiet AS hoadonchitiet2
    		INNER JOIN @GiaVonCapNhat AS _giavoncapnhat2 ON hoadonchitiet2.ID = _giavoncapnhat2.IDChiTietHoaDon    		
    		WHERE _giavoncapnhat2.LoaiHoaDon = 8 AND _giavoncapnhat2.RN > 1;
    
    			-- update GiaVon to Loai = 18 (Phieu DieuChinh GiaVon)
    		UPDATE hoadonchitiet3
    		SET hoadonchitiet3.DonGia = _giavoncapnhat3.GiaVonCu * _giavoncapnhat3.TyLeChuyenDoi, 
    				hoadonchitiet3.PTChietKhau = CASE WHEN hoadonchitiet3.GiaVon - (_giavoncapnhat3.GiaVonCu * _giavoncapnhat3.TyLeChuyenDoi) > 0 THEN hoadonchitiet3.GiaVon - (_giavoncapnhat3.GiaVonCu * _giavoncapnhat3.TyLeChuyenDoi) ELSE 0 END,
    			hoadonchitiet3.TienChietKhau = CASE WHEN hoadonchitiet3.GiaVon - (_giavoncapnhat3.GiaVonCu * _giavoncapnhat3.TyLeChuyenDoi) > 0 THEN 0 ELSE hoadonchitiet3.GiaVon - (_giavoncapnhat3.GiaVonCu * _giavoncapnhat3.TyLeChuyenDoi) END
    		FROM BH_HoaDon_ChiTiet AS hoadonchitiet3
    		INNER JOIN @GiaVonCapNhat AS _giavoncapnhat3
    		ON hoadonchitiet3.ID = _giavoncapnhat3.IDChiTietHoaDon
    		WHERE _giavoncapnhat3.LoaiHoaDon = 18 AND _giavoncapnhat3.RN > 1;
    
    		UPDATE chitietdinhluong
    		SET chitietdinhluong.GiaVon = gvDinhLuong.GiaVonDinhLuong / iif(chitietdinhluong.SoLuong=0,1,chitietdinhluong.SoLuong)
    		FROM BH_HoaDon_ChiTiet AS chitietdinhluong
    		INNER JOIN
    			(SELECT 
    					SUM(ct.GiaVon * ct.SoLuong) AS GiaVonDinhLuong, ct.ID_ChiTietDinhLuong 
    				FROM BH_HoaDon_ChiTiet ct
    			INNER JOIN (SELECT IDChiTietDinhLuong FROM @GiaVonCapNhat WHERE RN >1 GROUP BY IDChiTietDinhLuong) gv
    			ON (ct.ID_ChiTietDinhLuong = gv.IDChiTietDinhLuong and ct.ID_ChiTietDinhLuong IS NOT NULL)
    			WHERE gv.IDChiTietDinhLuong IS NOT NULL AND ct.ID != ct.ID_ChiTietDinhLuong
    			GROUP BY ct.ID_ChiTietDinhLuong) AS gvDinhLuong
    		ON chitietdinhluong.ID = gvDinhLuong.ID_ChiTietDinhLuong
			end try
			begin catch
				select error_number() as ErrNumber,				
					ERROR_LINE() as ErrLine,
					ERROR_MESSAGE() as ErrMsg
			end catch
    		----=========END Update BH_HoaDon_ChiTiet

    		--Update DM_GiaVon
    		UPDATE _dmGiaVon1
    		SET _dmGiaVon1.GiaVon = ISNULL(_gvUpdateDM1.GiaVon, 0)
    		FROM 
    				(SELECT dvqd1.ID AS IDDonViQuiDoi, _giavon1.IDLoHang AS IDLoHang, (CASE WHEN _giavon1.IDCheckIn = _giavon1.IDChiNhanhThemMoi THEN _giavon1.GiaVonNhan ELSE _giavon1.GiaVon END) * dvqd1.TyLeChuyenDoi AS GiaVon, _giavon1.IDChiNhanhThemMoi AS IDChiNhanh 
    				FROM @GiaVonCapNhat _giavon1
    			INNER JOIN (SELECT IDHangHoa,IDLoHang, MAX(RN) AS RN FROM @GiaVonCapNhat WHERE RN > 1 GROUP BY IDHangHoa,IDLoHang) AS _maxGiaVon1
    			ON _giavon1.IDHangHoa = _maxGiaVon1.IDHangHoa AND _giavon1.RN = _maxGiaVon1.RN AND (_giavon1.IDLoHang = _maxGiaVon1.IDLoHang OR _maxGiaVon1.IDLoHang IS NULL)
    			INNER JOIN DonViQuiDoi dvqd1
    			ON dvqd1.ID_HangHoa = _giavon1.IDHangHoa) AS _gvUpdateDM1
    		LEFT JOIN DM_GiaVon _dmGiaVon1
    		ON _gvUpdateDM1.IDChiNhanh = _dmGiaVon1.ID_DonVi AND _gvUpdateDM1.IDDonViQuiDoi = _dmGiaVon1.ID_DonViQuiDoi AND (_gvUpdateDM1.IDLoHang = _dmGiaVon1.ID_LoHang OR _gvUpdateDM1.IDLoHang IS NULL)
    		WHERE _dmGiaVon1.ID IS NOT NULL;
    
    		INSERT INTO DM_GiaVon (ID, ID_DonVi, ID_DonViQuiDoi, ID_LoHang, GiaVon)
    		SELECT NEWID(), _gvUpdateDM.IDChiNhanh, _gvUpdateDM.IDDonViQuiDoi, _gvUpdateDM.IDLoHang, _gvUpdateDM.GiaVon 
    			FROM 
    			(SELECT dvqd2.ID AS IDDonViQuiDoi, _giavon2.IDLoHang AS IDLoHang, 
    					(CASE WHEN _giavon2.IDCheckIn = _giavon2.IDChiNhanhThemMoi THEN _giavon2.GiaVonNhan ELSE _giavon2.GiaVon END) * dvqd2.TyLeChuyenDoi AS GiaVon, 
    					_giavon2.IDChiNhanhThemMoi AS IDChiNhanh 
    				FROM @GiaVonCapNhat _giavon2
    			INNER JOIN (SELECT IDHangHoa,IDLoHang, MAX(RN) AS RN FROM @GiaVonCapNhat WHERE RN >1 GROUP BY IDHangHoa, IDLoHang) AS _maxGiaVon
    		ON _giavon2.IDHangHoa = _maxGiaVon.IDHangHoa AND _giavon2.RN = _maxGiaVon.RN AND (_giavon2.IDLoHang = _maxGiaVon.IDLoHang OR _maxGiaVon.IDLoHang IS NULL)
    		INNER JOIN DonViQuiDoi dvqd2 ON dvqd2.ID_HangHoa = _giavon2.IDHangHoa) AS _gvUpdateDM    		
    		LEFT JOIN DM_GiaVon _dmGiaVon
    		ON _gvUpdateDM.IDChiNhanh = _dmGiaVon.ID_DonVi AND _gvUpdateDM.IDDonViQuiDoi = _dmGiaVon.ID_DonViQuiDoi AND (_gvUpdateDM.IDLoHang = _dmGiaVon.ID_LoHang OR _gvUpdateDM.IDLoHang IS NULL)
    		WHERE _dmGiaVon.ID IS NULL;
    		
			delete from BH_HoaDon_ChiTiet where ID_HoaDon = @IDHoaDonInput and ChatLieu='5'

    		END
    		--END TinhGiaVonTrungBinh
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
    	declare @PTNNew_IDCusNew uniqueidentifier, @PTNNew_BaoHiem uniqueidentifier
    	select @PTNNew_IDCusNew = ID_KhachHang, @PTNNew_BaoHiem = ID_BaoHiem from Gara_PhieuTiepNhan where ID= @ID_PhieuTiepNhan
    
    	---- get list hoadon of PTN
    	select ID, ID_DoiTuong, ID_BaoHiem
    	into #tblHoaDon
    	from BH_HoaDon
    	where ID_PhieuTiepNhan = @ID_PhieuTiepNhan
    	and ChoThanhToan is not null
    	and LoaiHoaDon in (3,25)
    
    	---- update cus
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

			Sql(@"ALTER PROCEDURE [dbo].[PTN_CheckChangeCus]
    @ID_PhieuTiepNhan [uniqueidentifier],
    @ID_KhachHangNew [uniqueidentifier],
    @ID_BaoHiemNew [nvarchar](40)
AS
BEGIN
    SET NOCOUNT ON;
    
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
    	and ChoThanhToan is not null
    	and LoaiHoaDon in (3,25)
   
    
    	if @ID_KhachHangNew != @PTNOld_IDCus
    	begin
    		declare @count1 int;
    		select @count1 =count(*)
    		from #tblHoaDon
    		where ID_DoiTuong != @ID_KhachHangNew
    
    		if @count1 > 0
    			insert into @tblReturn values (1)

			---- check exist soquy khachhang
    		declare @countSQKH int
    		select @countSQKH = count(qhd.ID)
    		from #tblHoaDon hd
    		join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan
    		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    		where (qhd.TrangThai is null or qhd.TrangThai= 1)
			and qct.ID_DoiTuong= @PTNOld_IDCus
    
    		if @countSQKH > 0
    			 insert into @tblReturn values (3)
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

			---- check exist soquy baohiem
    		declare @countSQBH int
    		select @countSQBH = count(qhd.ID)
    		from #tblHoaDon hd
    		join Quy_HoaDon_ChiTiet qct on hd.ID = qct.ID_HoaDonLienQuan
    		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    		where (qhd.TrangThai is null or qhd.TrangThai= 1)
			and qct.ID_DoiTuong= @PTNOld_BaoHiem
    
    		if @countSQBH > 0
    			 insert into @tblReturn values (4)
    	end      	
    
    	 select * from @tblReturn
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListHoaDonSuaChua]
    @IDChiNhanhs [nvarchar](max),
    @FromDate datetime,
    @ToDate datetime,
    @ID_PhieuSuaChua [nvarchar](max),
    @IDXe [uniqueidentifier],
    @TrangThais [nvarchar](20),
    @TextSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	if @FromDate = '2016-01-01' 
    		set @ToDate= (select DATEADD(day,1, max(NgayLapHoaDon)) from BH_HoaDon where LoaiHoaDon= 25)

			if isnull(@ID_PhieuSuaChua,'')=''
				set @ID_PhieuSuaChua ='%%'

		select ID , ID_DoiTuong, ID_BaoHiem, ID_HoaDon,NgayLapHoaDon, ChoThanhToan
		into #tmpHDSC
		from BH_HoaDon hd
		where hd.ID_PhieuTiepNhan like @ID_PhieuSuaChua
		and hd.NgayLapHoaDon between @FromDate and @ToDate
		and hd.LoaiHoaDon = 25
    
    	declare @tblDonVi table (ID_DonVi uniqueidentifier)
    	if(@IDChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IDChiNhanhs)
    	END
    	ELSE
    	BEGIN
    		INSERT INTO @tblDonVi
    		SELECT ID FROM DM_DonVi;
    	END
    
    	declare @tbTrangThai table (GiaTri varchar(2))
    	insert into @tbTrangThai
    	select Name from dbo.splitstring(@TrangThais)
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);	

	if isnull(@PageSize,0) = 0 set @PageSize = 99999999
	

	;with data_cte
	as
	(
	
		select *
		from
		(
			select 
					 hd.ID,
					 hd.ID_DonVi,
					 hd.NguoiTao, hd.ID_NhanVien,
					 hd.ID_BangGia,
					 hd.loaihoadon,hd.MaHoaDon, hd.ID_HoaDon, hd.ID_DoiTuong, hd.NgayLapHoaDon, 
					 hd.NguoiTao as NguoiTaoHD,
					 hd.SoVuBaoHiem, 
					
					  isnull(hd.TongTienBHDuyet,0) as TongTienBHDuyet, 
					 isnull(hd.PTThueHoaDon,0) as PTThueHoaDon, 
					  isnull(hd.PTThueBaoHiem,0) as PTThueBaoHiem, 
					  isnull(hd.TongTienThueBaoHiem,0) as TongTienThueBaoHiem, 
					   isnull(hd.KhauTruTheoVu,0) as KhauTruTheoVu, 
					   
					isnull(hd.TongThueKhachHang,0) as  TongThueKhachHang,
					isnull(hd.CongThucBaoHiem,0) as  CongThucBaoHiem,
					hd.GiamTruThanhToanBaoHiem as  GiamTruThanhToanBaoHiem,

					    isnull(hd.PTGiamTruBoiThuong,0) as PTGiamTruBoiThuong, 
					 isnull(hd.GiamTruBoiThuong,0) as GiamTruBoiThuong, 
					  isnull(hd.BHThanhToanTruocThue,0) as BHThanhToanTruocThue, 
					  isnull(hd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem, 													
					hd.ID_CheckIn, 
					hd.DienGiai,
					 hd.NgaySua, hd.NgayTao, hd.ID_PhieuTiepNhan,
					hd.ID_BaoHiem, hd.TongTienHang, hd.PhaiThanhToan, hd.TongThanhToan, hd.TongGiamGia,
					hd.TongTienThue,
					isnull(hd.ChiPhi,0) as TongChiPhi ,
					hd.TongChietKhau,
					hd.ChoThanhToan, 
					hd.YeuCau,
						xe.BienSo,
    				tn.MaPhieuTiepNhan,
					tn.SoKmVao,
					tn.SoKmRa,
    				dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai as DienThoaiKhachHang, dt.Email, dt.DiaChi, 
					dt.MaSoThue,
					dt.TaiKhoanNganHang,
					 nv.TenNhanVien,
					 iif(hd.ID_BaoHiem is null,'',tn.NguoiLienHeBH) as LienHeBaoHiem,
					iif(hd.ID_BaoHiem is null,'',tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,
    				ISNULL(bg.MaHoaDon,'') as MaBaoGia,
					isnull(bh.TenDoiTuong,'') as TenBaoHiem,
					isnull(bh.MaDoiTuong,'') as MaBaoHiem,
    				isnull(bh.Email,'') as BH_Email,
    				isnull(bh.DiaChi,'') as BH_DiaChi,
    				isnull(bh.DienThoai,'') as DienThoaiBaoHiem,
					
    				case hd.ChoThanhToan
    					when 0 then '0'
    					when 1 then '1'
    					else '2' end as TrangThai,
    				case hd.ChoThanhToan
    					when 0 then N'Hoàn thành'
    					when 1 then N'Phiếu tạm'
    					else N'Đã hủy'
    					end as TrangThaiText,

						hdThuChi.Khach_TienMat,
						hdThuChi.Khach_TienPOS,
						hdThuChi.Khach_TienCK,
						hdThuChi.Khach_TheGiaTri,
						hdThuChi.Khach_TienDiem,
						hdThuChi.Khach_TienCoc,

						hdThuChi.BH_TienMat,
						hdThuChi.BH_TienPOS,
						hdThuChi.BH_TienCK,
						hdThuChi.BH_TheGiaTri,
						hdThuChi.BH_TienDiem,
						hdThuChi.BH_TienCoc,
						hdThuChi.KhachDaTra,
						hdThuChi.BaoHiemDaTra,
						hdThuChi.ThuDatHang

	from
	(
				select 
					hdsq.ID,
					sum(hdsq.Khach_TienMat) as Khach_TienMat,
					sum(hdsq.Khach_TienPOS) as Khach_TienPOS,
					sum(hdsq.Khach_TienCK) as Khach_TienCK,
					sum(hdsq.Khach_TheGiaTri) as Khach_TheGiaTri,
					sum(hdsq.Khach_TienDiem) as Khach_TienDiem,
					sum(hdsq.Khach_TienCoc) as Khach_TienCoc,
					sum(hdsq.BH_TienMat) as BH_TienMat,
					sum(hdsq.BH_TienPOS) as BH_TienPOS,
					sum(hdsq.BH_TienCK) as BH_TienCK,
					sum(hdsq.BH_TheGiaTri) as BH_TheGiaTri,
					sum(hdsq.BH_TienDiem) as BH_TienDiem,
					sum(hdsq.BH_TienCoc) as BH_TienCoc,
					sum(hdsq.KhachDaTra) as KhachDaTra,
					sum(hdsq.BaoHiemDaTra) as BaoHiemDaTra,
					sum(hdsq.ThuDatHang) as ThuDatHang
				from
				(
				select 
					hd.ID,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0)) as Khach_TienMat,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0))as Khach_TienPOS,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0)) as Khach_TienCK,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0)) as Khach_TheGiaTri,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0)) as Khach_TienDiem,
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=6, qct.TienThu, 0)) as Khach_TienCoc,	
					iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as KhachDaTra,
					0 as ThuDatHang,
					0 as BH_TienMat,
					0 as BH_TienPOS,
					0 as BH_TienCK,
					0 as BH_TheGiaTri,
					0 as BH_TienDiem,
					0 as BH_TienCoc,
					0 as BaoHiemDaTra					
				from #tmpHDSC hd
				--(
				--	select ID , ID_DoiTuong
				--	from BH_HoaDon hd
				--	where hd.ID_PhieuTiepNhan like @ID_PhieuSuaChua
				--	and hd.NgayLapHoaDon between @FromDate and @ToDate
				--	and hd.LoaiHoaDon = 25
				--)  hd
				left join Quy_HoaDon_ChiTiet qct on hd.ID= qct.ID_HoaDonLienQuan and qct.ID_DoiTuong= hd.ID_DoiTuong
				left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
			
	
				union all

				select 
					thuDH.ID,										
					isnull(thuDH.Khach_TienMat,0) as Khach_TienMat,
					isnull(thuDH.Khach_TienPOS,0) as Khach_TienPOS,
					isnull(thuDH.Khach_TienCK,0) as Khach_TienCK,
					isnull(thuDH.Khach_TheGiaTri,0) as Khach_TheGiaTri,
					isnull(thuDH.Khach_TienDiem,0) as Khach_TienDiem,
					isnull(thuDH.Khach_TienCoc,0) as Khach_TienCoc,
					isnull(thuDH.ThuDatHang,0) as KhachDaTra,
					isnull(thuDH.ThuDatHang,0) as ThuDatHang,
					0 as BH_TienMat,
					0 as BH_TienPOS,
					0 as BH_TienCK,
					0 as BH_TheGiaTri,
					0 as BH_TienDiem,
					0 as BH_TienCoc,
					0 as BaoHiemDaTra
					
					from
					(	
							Select 
								ROW_NUMBER() OVER(PARTITION BY ID_HoaDon ORDER BY NgayLapHoaDon ASC) AS isFirst,						
    							d.ID,
								d.ID_HoaDon,
								d.NgayLapHoaDon,    								
    							sum(d.Khach_TienMat) as Khach_TienMat,	
    							sum(d.Khach_TienPOS) as Khach_TienPOS,																	
    							sum(d.Khach_TienCK) as Khach_TienCK,							
										
    							sum(d.Khach_TheGiaTri) as Khach_TheGiaTri,																	
    							sum(d.Khach_TienDiem) as Khach_TienDiem,																	
								sum(d.Khach_TienCoc) as Khach_TienCoc,																	
    							sum(d.TienThu) as ThuDatHang																								
    						FROM
    						(
									
									select hd.ID, hd.NgayLapHoaDon, hd.ID_HoaDon,
										iif(qct.HinhThucThanhToan=1, qct.TienThu, 0) as Khach_TienMat,
										iif(qct.HinhThucThanhToan=2, qct.TienThu, 0) as Khach_TienPOS,
										iif(qct.HinhThucThanhToan=3, qct.TienThu, 0) as Khach_TienCK,
										iif(qct.HinhThucThanhToan=4, qct.TienThu, 0) as Khach_TheGiaTri,
										iif(qct.HinhThucThanhToan=5, qct.TienThu, 0) as Khach_TienDiem,
										iif(qct.HinhThucThanhToan=6, qct.TienThu, 0) as Khach_TienCoc,		
										iif(qhd.LoaiHoaDon = 11,qct.TienThu, -qct.TienThu) as TienThu												
									from #tmpHDSC hd
									--(
									--	select ID, NgayLapHoaDon, ID_HoaDon
									--	from dbo.BH_HoaDon 
									--	where ID_PhieuTiepNhan like @ID_PhieuSuaChua 										
									--	and ChoThanhToan ='0'
									--	and LoaiHoaDon = 25
									--)  hd
									join BH_HoaDon hdd on hdd.ID= hd.ID_HoaDon
									join Quy_HoaDon_ChiTiet qct on hdd.ID= qct.ID_HoaDonLienQuan
									join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID																				
									where hdd.LoaiHoaDon= 3																
									and qhd.TrangThai= 1	
									and hd.ChoThanhToan='0'
																	
    						) d group by d.ID,d.NgayLapHoaDon,ID_HoaDon						
						) thuDH
					where isFirst= 1

					union all

						select 
								hd.ID,
								0 as Khach_TienMat,
								0 as Khach_TienPOS,
								0 as Khach_TienCK,
								0 as Khach_TheGiaTri,
								0 as Khach_TienDiem,
								0 as Khach_TienCoc,
								0 as KhachDaTra, 
								0 as ThuDatHang,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0)) as BH_TienMat,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0))as BH_TienPOS,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0)) as BH_TienCK,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0)) as BH_TheGiaTri,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0)) as BH_TienDiem,
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qct.HinhThucThanhToan=6, qct.TienThu, 0)) as BH_TienCoc,	
							iif(qhd.ID is null or qhd.TrangThai ='0',0, iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as BaoHiemDaTra
							
					from #tmpHDSC hd
					--(
					--	select ID , ID_BaoHiem
					--	from BH_HoaDon hd
					--	where hd.ID_PhieuTiepNhan like @ID_PhieuSuaChua
					--	and hd.NgayLapHoaDon between @FromDate and @ToDate
					--	and hd.LoaiHoaDon = 25
					--)  hd
				left join Quy_HoaDon_ChiTiet qct on hd.ID= qct.ID_HoaDonLienQuan and qct.ID_DoiTuong= hd.ID_BaoHiem
				left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
			
				
	) hdsq
	group by hdsq.ID
	) hdThuChi
	join BH_HoaDon hd on hdThuChi.ID= hd.ID
	left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID   
	join Gara_PhieuTiepNhan tn on tn.ID= hd.ID_PhieuTiepNhan
	left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
	left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
	left join DM_DoiTuong bh on hd.ID_BaoHiem= bh.ID
	left join BH_HoaDon bg on hd.ID_HoaDon= bg.ID and bg.ID_PhieuTiepNhan= tn.ID
	where  exists (select ID_DonVi from @tblDonVi dv where hd.ID_DonVi = dv.ID_DonVi) 
		and (@IDXe IS NULL OR @IDXe = tn.ID_Xe)
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
	) hd where exists (select GiaTri from @tbTrangThai tt where hd.TrangThai = tt.GiaTri)
),
	count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    			from data_cte
    		),
			tView
			as
			(
			select dt.*, cte.*
			from data_cte dt   		
    		cross join count_cte cte
			order by dt.NgayLapHoaDon desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
			)

			select *
			into #tblView
			from tView

			----- get list ID of top 10
		declare @tblID TblID
		insert into @tblID
		select ID from #tblView
		
		------ get congno of top 10
		declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, TongTienHDTra float, KhachDaTra float)
		insert into @tblCongNo
		exec HD_GetBuTruTraHang @tblID, 6
					
		
		select tView.*,
			cn.MaHoaDonGoc,
			cn.LoaiHoaDonGoc,
			isnull(cn.TongTienHDTra,0) as TongTienHDTra,			
			iif(tView.ChoThanhToan is null,0, tView.TongThanhToan - isnull(cn.TongTienHDTra,0)- tView.KhachDaTra - tView.BaoHiemDaTra) as ConNo
		from #tblView tView
		left join @tblCongNo cn on tView.ID = cn.ID
		order by tView.NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[LoadDanhMucHangHoa]
   @IDChiNhanh uniqueidentifier ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
   @TextSearch nvarchar(max) ='',
   @IDThuocTinhHangs nvarchar(max)='', 
   @IDViTriKhos nvarchar(max) ='',
   @TrangThaiKho int=0, 
   @Where nvarchar(max) ='',
   @CurrentPage int = 0,
   @PageSize int = 1000,
   @ColumnSort varchar(100) ='NgayTao',
   @SortBy varchar(20) = 'DESC'
AS
BEGIN
	SET NOCOUNT ON;

	declare @where1 nvarchar(max), @where2 nvarchar(max), 
		@paramDefined nvarchar(max),
    	@sql1 nvarchar(max) ='', @sql2 nvarchar(max) =''
    	declare @tblDefined nvarchar(max) = concat(N' declare @tblThuocTinh table (ID uniqueidentifier) ',	
												   N' declare @tblViTri table (ID uniqueidentifier) ',
    											   N' declare @tblSearch table (Name nvarchar(max)) ')


	set @where1 =' where 1 = 1 and qd.LaDonViChuan = 1 '
    set @where2 =' where 1 = 1'
	
	
	if isnull(@Where,'')!=''
		set @Where = CONCAT(' and ',N'', @Where)

	if isnull(@ColumnSort,'')=''
		set @ColumnSort = 'NgayTao'
	if isnull(@SortBy,'')=''
		set @SortBy = 'DESC'
    
	if isnull(@TextSearch,'')!=''
		begin
			set @sql1 = concat(@sql1,
				N'DECLARE @count int;
				INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!='''';
				Select @count =  (Select count(*) from @tblSearch) ')

			set @where1 = CONCAT(@where1, N' and
						((select count(*) from @tblSearch b where 
    								hh.TenHangHoa_KhongDau like ''%''+b.Name+''%''
    								or hh.TenHangHoa_KyTuDau like ''%''+b.Name+''%'' 
									or hh.TenHangHoa like ''%''+b.Name+''%''
									or hh.GhiChu like ''%'' +b.Name +''%'' 
    								or qd.MaHangHoa like ''%''+b.Name+''%'' )=@count or @count=0)')

		end

	if isnull(@IDThuocTinhHangs,'')!=''
		begin
			set @sql1 = concat(@sql1, N' insert into @tblThuocTinh select name from dbo.splitstring(@IDThuocTinhHangs_In) where Name!='' ''')		
			set @where1 = CONCAT(@where1, N' and exists
									(select * 
									from HangHoa_ThuocTinh tt 
									where hh.ID = tt.ID_HangHoa 
									and exists (select ID from @tblThuocTinh prop where tt.ID = prop.ID) 
									)')
		end
	if isnull(@IDViTriKhos,'')!=''
		begin
			set @sql1 = concat(@sql1, N' insert into @tblViTri select name from dbo.splitstring(@IDViTriKhos_In) where Name!='' ''')		
			set @where1 = CONCAT(@where1, N' and exists
									(select vth.ID 
									from DM_ViTriHangHoa vth 
									where hh.ID = vth.ID_HangHoa 
									and exists (select ID from @tblViTri vt where vt.ID = vth.ID_ViTri) 
									)')
		end
	
	if isnull(@TrangThaiKho,0)!=0
		begin			
			if @TrangThaiKho in (1,5,6)
				set @where2 = CONCAT(@where2, N' and tblOut.TrangThai_TonKho = @TrangThaiKho_In ') 
			if @TrangThaiKho in (3,4)
				set @where2 = CONCAT(@where2, N' and tblOut.TrangThai_DinhMucTon = @TrangThaiKho_In ') 
		end
	
	set @sql2= concat( N'
;with data_cte
 as
 (
	select *,
		tblOut.TheoDoi as TrangThai,
		case tblOut.LoaiHangHoa 
			when 1 then N''Hàng hóa''
			when 2 then N''Dịch vụ''
			when 3 then N''Combo''
		end as sLoaiHangHoa
	from
	(
	select tbl.*,
		cast(tbl.LaChaCungLoai1 as bit) LaChaCungLoai,
		tbl.TenDonViTinh as DonViTinhChuan,

		iif(tbl.CountCungLoai = 1 or QuanLyTheoLoHang =''1'', tbl.MaHangHoa1, concat(''('',tbl.CountCungLoai, N'') Mã hàng'')) as MaHangHoa,
		iif(tbl.LoaiHangHoa=1, tbl.GiaVon1, dbo.GetGiaVonOfDichVu(@IDChiNhanh_In,tbl.ID_DonViQuiDoi)) as GiaVon	,
		------0.all
		----- 1.tonkho > 0
		----- 2.tonkho <=0 (bo qua cai nay)
		----- 3.Dưới định mức tồn
		----- 4.Vượt định mức tồn
		----- 5.Hàng âm kho
		----- 6.TonKho = 0
		case
			when tbl.TonKho > 0 then 1
			when tbl.TonKho < 0 then 5
		else 6 end as TrangThai_TonKho,
		case
			when tbl.TonKho < tbl.TonToiThieu then 3
			when tbl.TonKho > tbl.TonToiDa then 4
		else 0 end as TrangThai_DinhMucTon
	from
	(
			select 			
				
				tblGr.LaHangHoa,				
				tblGr.DuocBanTrucTiep,
				tblGr.TheoDoi,
				tblGr.Xoa,
				tblGr.QuanLyTheoLoHang,
				tblGr.ID_HangHoaCungLoai,
				tblGr.ChietKhauMD_NVTheoPT,
				tblGr.DuocTichDiem,
				iif(tblGr.QuanLyTheoLoHang = 1, 1, count(tblGr.ID_HangHoaCungLoai)) as CountCungLoai,

				min(tblGr.ID) as ID,		---- trường hợp sửa lại all thông tin hàng cùng loại
				min(tblGr.ID_DonViQuiDoi) as ID_DonViQuiDoi,
				min(tblGr.LaChaCungLoai) as LaChaCungLoai1,		
				min(tblGr.MaHangHoa) as MaHangHoa1,
				min(tblGr.NgayTao) as NgayTao,				
				min(tblGr.TenHangHoa) as TenHangHoa, 
				min(tblGr.TenHangHoa_KhongDau) as TenHangHoa_KhongDau,			
				min(tblGr.TenDonViTinh) as TenDonViTinh,
				min(tblGr.ThuocTinhGiaTri) as ThuocTinhGiaTri,							
				min(tblGr.GhiChu) as GhiChu,										
				min(tblGr.TrangThaiKinhDoanh) as TrangThaiKinhDoanh,
				min(tblGr.TrangThaiHang) as TrangThaiHang,
				min(tblGr.ID_Xe) as ID_Xe,							
				min(tblGr.ID_NhomHangHoa) as ID_NhomHangHoa,
				min(tblGr.NhomHangHoa) as NhomHangHoa,			
				min(tblGr.LoaiHangHoa) as LoaiHangHoa,
				min(tblGr.SoPhutThucHien) as SoPhutThucHien,
				min(tblGr.DichVuTheoGio) as DichVuTheoGio,
				min(tblGr.ChietKhauMD_NV) as ChietKhauMD_NV,				
				min(tblGr.QuanLyBaoDuong) as QuanLyBaoDuong,
				min(tblGr.LoaiBaoDuong) as LoaiBaoDuong,
				min(tblGr.SoKmBaoHanh) as SoKmBaoHanh,
				min(tblGr.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,
				min(tblGr.TonToiDa) as TonToiDa,
				min(tblGr.TonToiThieu) TonToiThieu,				
				min(tblGr.GiaBan) as GiaBan,
				
				max(tblGr.GiaVon) as GiaVon1,
				sum(tblGr.TonKho) as TonKho		
			from
			(
				select 
					hh.ID,				
					hh.ID_Xe,
					qd.ID as ID_DonViQuiDoi,
					hh.TenHangHoa,
					hh.TenHangHoa_KhongDau,
					qd.MaHangHoa,
					hh.LaHangHoa,
					hh.GhiChu,				
					cast(hh.LaChaCungLoai as int) LaChaCungLoai,
					hh.DuocBanTrucTiep,
					hh.TheoDoi,
					hh.NgayTao,
					hh.ID_HangHoaCungLoai,
					hh.ID_NhomHang as ID_NhomHangHoa,
					nhom.TenNhomHangHoa as NhomHangHoa,
			
					iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa=''1'',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
					isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
					isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,	
					isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
					isnull(hh.ChietKhauMD_NVTheoPT,''0'') as ChietKhauMD_NVTheoPT,
					isnull(hh.DuocTichDiem,0) as DuocTichDiem,
					iif(hh.QuanLyTheoLoHang is null,''0'', hh.QuanLyTheoLoHang) as QuanLyTheoLoHang,
					iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
					iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
					iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,
					iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
					isnull(hh.TonToiDa,0) as TonToiDa,
					isnull(hh.TonToiThieu,0) as TonToiThieu,
				
					qd.GiaBan,
					qd.Xoa,
					qd.TenDonViTinh,
					qd.ThuocTinhGiaTri,
					iif(hh.TheoDoi=''1'',1,2) as TrangThaiKinhDoanh, ----- 0.all, 1.dangkinhdoanh, 2.ngungkinhdoanh
					iif(qd.Xoa=''1'',1,0) as TrangThaiHang,
					ISNULL(tk.TonKho,0) as TonKho,
					isnull(gv.GiaVon,0) as GiaVon ',						
					
				N' from DM_HangHoa hh 	
				left join DonViQuiDoi qd on qd.ID_HangHoa= hh.ID				
				left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID		
				left join DM_HangHoa_TonKho tk on qd.ID = tk.ID_DonViQuyDoi and tk.ID_DonVi= @IDChiNhanh_In
				left join DM_GiaVon gv on qd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi= @IDChiNhanh_In	
				and (gv.ID_LoHang = tk.ID_LoHang or hh.QuanLyTheoLoHang =''0'')				
				
	', @where1,		
	 N'  ) tblGr
		 group by 
			tblGr.LaHangHoa, ------ hangcungloai: chi lay 1 dong				
			tblGr.DuocBanTrucTiep,
			tblGr.Xoa,
			tblGr.QuanLyTheoLoHang,						
			tblGr.TheoDoi,
			tblGr.ID_HangHoaCungLoai,
			tblGr.ChietKhauMD_NVTheoPT,
			tblGr.DuocTichDiem
		) tbl		
		
	) tblOut ', @where2, @Where,
	N'),
count_cte
as
(
	select COUNT(ID) as TotalRow,
		ceiling(COUNT(ID)/ cast (@PageSize_In as float)) as TotalPage,
		sum(TonKho) as SumTonKho
	from data_cte
)

select dt.*,count_cte.*,
	tblVTtri.TenViTris,
	xe.BienSo
from data_cte dt
cross join count_cte 
left join Gara_DanhMucXe xe on dt.ID_Xe= xe.ID
left join
(
----- vitrikho -----
	select hh.ID,
			(
    		Select  vt.TenViTri + '','' AS [text()]
    		From dbo.DM_HangHoa_ViTri vt
			join dbo.DM_ViTriHangHoa vth on vt.ID= vth.ID_ViTri
    		where hh.ID= vth.ID_HangHoa
    		For XML PATH ('''')
		 ) TenViTris
	From data_cte hh
) tblVTtri on dt.ID = tblVTtri.ID
order by 
	case when @SortBy_In <> ''ASC'' then ''''
	when @ColumnSort_In=''NgayTao'' then dt.NgayTao end ASC,
	case when @SortBy_In <> ''DESC'' then ''''
	when @ColumnSort_In=''NgayTao'' then dt.NgayTao end DESC,
	case when @SortBy_In <> ''ASC'' then ''''
	when @ColumnSort_In=''MaHangHoa'' then MaHangHoa1 end ASC,
	case when @SortBy_In <> ''DESC'' then ''''
	when @ColumnSort_In=''MaHangHoa'' then MaHangHoa1 end DESC,
	case when @SortBy_In <> ''ASC'' then ''''
	when @ColumnSort_In=''TenHangHoa'' then TenHangHoa end ASC,
	case when @SortBy_In <> ''DESC'' then ''''
	when @ColumnSort_In=''TenHangHoa'' then TenHangHoa end DESC,
	case when @SortBy_In <> ''ASC'' then ''''
	when @ColumnSort_In=''TenNhomHang'' then NhomHangHoa end ASC,
	case when @SortBy_In <> ''DESC'' then ''''
	when @ColumnSort_In=''TenNhomHang'' then NhomHangHoa end DESC,
	case when @SortBy_In <> ''ASC'' then 0
	when @ColumnSort_In=''GiaBan'' then GiaBan end ASC,
	case when @SortBy_In <> ''DESC'' then 0
	when @ColumnSort_In=''GiaBan'' then GiaBan end DESC,
	case when @SortBy_In <> ''ASC'' then 0
	when @ColumnSort_In=''GiaVon'' then GiaVon1 end ASC,
	case when @SortBy_In <> ''DESC'' then 0
	when @ColumnSort_In=''GiaVon'' then GiaVon1 end DESC,
	case when @SortBy_In <> ''ASC'' then 0
	when @ColumnSort_In=''TonKho'' then TonKho end ASC,
	case when @SortBy_In <> ''DESC'' then 0
	when @ColumnSort_In=''TonKho'' then TonKho end DESC
	OFFSET (@CurrentPage_In* @PageSize_In) ROWS
	FETCH NEXT @PageSize_In ROWS ONLY

'
)
	

	set @paramDefined = N' @IDChiNhanh_In uniqueidentifier,
    								@TextSearch_In nvarchar(max),
    								@IDThuocTinhHangs_In nvarchar(max),
									@IDViTriKhos_In nvarchar(max),
    								@TrangThaiKho_In int,    							
    								@Where_In nvarchar(max),		
    								@CurrentPage_In int,
    								@PageSize_In int,
									@ColumnSort_In varchar(100),
    								@SortBy_In varchar(20)'

	set @sql2 = CONCAT(@tblDefined, @sql1, @sql2)
    
	exec sp_executesql @sql2, 
    		@paramDefined,
    		@IDChiNhanh_In = @IDChiNhanh,
    		@TextSearch_In = @TextSearch,
    		@IDThuocTinhHangs_In = @IDThuocTinhHangs,
			@IDViTriKhos_In = @IDViTriKhos,
    		@TrangThaiKho_In = @TrangThaiKho,   	
    		@Where_In = @Where, 	
    		@CurrentPage_In = @CurrentPage,
    		@PageSize_In = @PageSize,
			@ColumnSort_In = @ColumnSort,
			@SortBy_In = @SortBy


			
			

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetChiTietHoaDon_afterTraHang]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    set nocount on
    
    	---- get cthdmua
    	select ID		
    	into #temCTMua
    	from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
    
    	---- get cttra or ctsudung
    	select 
    		ct.ID_ChiTietGoiDV,
    		SUM(ct.SoLuong) as SoLuongTra
    	into #tmpHDTra
    	from BH_HoaDon_ChiTiet ct 
    	join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    	where hd.ChoThanhToan='0' and hd.LoaiHoaDon not in (8,2)
    	and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    	and exists (select ctm.ID from #temCTMua ctm where ct.ID_ChiTietGoiDV= ctm.ID)
    	group by ct.ID_ChiTietGoiDV
    
    
    	---- get soluong xuatkho of hdsc
    		select SUM(ctxk.SoLuong) as SoLuongXuat, ctxk.ID_ChiTietGoiDV
    		into #ctxk
    		from BH_HoaDon_ChiTiet ctxk 
    		join BH_HoaDon hdxk on ctxk.ID_HoaDon = hdxk.ID
    		where hdxk.ID_HoaDon = @ID_HoaDon
    		and hdxk.LoaiHoaDon = 8 and hdxk.ChoThanhToan='0'		
    		group by ctxk.ID_ChiTietGoiDV			
    			
    
    
    select  distinct
    		CAST(ctm.SoThuTu as float) as SoThuTu,
    		ctm.ID, 
    		ctm.ID_DonViQuiDoi,
    		ctm.ID_LoHang,
    		ctm.ID_TangKem, 
    		ctm.TangKem, 
    		ctm.ID_ParentCombo,
    		ctm.ID_ChiTietDinhLuong,
    		ctm.SoLuong,
    		ISNULL(ctt.SoLuongTra,0) as SoLuongTra,
    		iif(hd.LoaiHoaDon =25 and hh.LaHangHoa ='1', 
    		isnull(xk.SoLuongXuat,0) - ISNULL(ctt.SoLuongTra,0) ,ctm.SoLuong - ISNULL(ctt.SoLuongTra,0)) as SoLuongConLai,
    		ctm.DonGia, isnull(gv.GiaVon,0) as GiaVon, ctm.ThanhTien, ctm.ThanhToan, 
    		ctm.TienChietKhau, 
    		ctm.TienChietKhau as GiamGia,
    		ctm.ThoiGian, ctm.GhiChu, ctm.PTChietKhau,
    		ctm.ID_HoaDon, ctm.ID_ViTri,
    		ctm.ID_LichBaoDuong,
    		isnull(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
    		CAST(ISNULL(ctm.TienThue,0) as float) as TienThue,CAST(ISNULL(ctm.PTThue,0) as float) as PTThue, 
    		CAST(ISNULL(ctm.ThoiGianBaoHanh,0) as float) as ThoiGianBaoHanh,
    		CAST(ISNULL(ctm.LoaiThoiGianBH,0) as float) as LoaiThoiGianBH,
    		Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end as PhiDichVu,
    			Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
    		lo.NgaySanXuat, lo.NgayHetHan, isnull(lo.MaLoHang,'') as MaLoHang, isnull(tk.TonKho,0) as TonKho,
    		hh.QuanLyTheoLoHang,
    		hh.LaHangHoa,
    		iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
    		hd.MaHoaDon,
    		hh.DichVuTheoGio,
    		hh.DuocTichDiem,
    		hh.SoPhutThucHien,
			isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
    		isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
    		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		qd.TenDonViTinh,
			qd.ID_HangHoa,
			qd.MaHangHoa,
    		ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan,
    		CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, 
    		hh.TenHangHoa, 
    		CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach, 
    		hh.ID_NhomHang as ID_NhomHangHoa, 
    		ISNULL(hh.GhiChu,'') as GhiChuHH,
    		ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
    		isnull(ctm.DonGiaBaoHiem,0) as DonGiaBaoHiem,
    		iif(ctm.TenHangHoaThayThe is null or ctm.TenHangHoaThayThe ='', hh.TenHangHoa, ctm.TenHangHoaThayThe) as TenHangHoaThayThe		
    
    	from BH_HoaDon_ChiTiet ctm
    	join BH_HoaDon hd on ctm.ID_HoaDon= hd.ID
    	join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID 
    	join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
    	LEFT JOIN DM_LoHang lo ON ctm.ID_LoHang = lo.ID
    	left join DM_HangHoa_TonKho tk on (ctm.ID_DonViQuiDoi = tk.ID_DonViQuyDoi and (ctm.ID_LoHang = tk.ID_LoHang or ctm.ID_LoHang is null) and  tk.ID_DonVi = hd.ID_DonVi)
    	left join DM_GiaVon gv on (tk.ID_DonViQuyDoi = gv.ID_DonViQuiDoi and (ctm.ID_LoHang = gv.ID_LoHang or ctm.ID_LoHang is null) and gv.ID_DonVi = hd.ID_DonVi) 
    	left join  #tmpHDTra ctt  on ctm.ID = ctt.ID_ChiTietGoiDV 
    	left join #ctxk xk on ctm.ID = xk.ID_ChiTietGoiDV
    	where ctm.ID_HoaDon= @ID_HoaDon
    	and (ctm.ID_ChiTietDinhLuong is null or ctm.ID_ChiTietDinhLuong = ctm.ID)
    	and (ctm.ID_ParentCombo is null or ctm.ID_ParentCombo = ctm.ID)
    	and (hh.LaHangHoa = 0 or (hh.LaHangHoa = 1 and tk.TonKho is not null))
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetChiTietHoaDon_ByIDHoaDon]
   @ID_HoaDon [uniqueidentifier]
AS
BEGIN
  set nocount on;

  declare @ID_DonVi uniqueidentifier, @loaiHD int, @ID_HoaDonGoc uniqueidentifier		
	select top 1 @ID_DonVi= ID_DonVi, @ID_HoaDonGoc= ID_HoaDon, @loaiHD= LoaiHoaDon from BH_HoaDon where ID= @ID_HoaDon

  if @loaiHD= 8
		begin
		select 
			ctxk.ID,ctxk.ID_DonViQuiDoi,ctxk.ID_LoHang,
			ctxk.SoLuong,
			ctxk.SoLuong as SoLuongXuatHuy,
			ctxk.DonGia,
			ctxk.GiaVon, 
			ctxk.GiaTriHuy as ThanhTien, 
			ctxk.GiaTriHuy as ThanhToan, 
			ctxk.TienChietKhau as GiamGia,
			ctxk.GhiChu,
			cast(ctxk.SoThuTu as float) as SoThuTu,
			hd.MaHoaDon,
			hd.NgayLapHoaDon,
			hd.ID_NhanVien,
    		nv.TenNhanVien,
			lh.NgaySanXuat,
    		lh.NgayHetHan,    			
    		dvqd.MaHangHoa,
    		hh.TenHangHoa,
			hh.TenHangHoa as TenHangHoaThayThe,
			Case when hh.QuanLyTheoLoHang is null then 'false' else hh.QuanLyTheoLoHang end as QuanLyTheoLoHang, 
    		concat(hh.TenHangHoa , '', dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    		dvqd.TenDonViTinh as TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		Case when lh.MaLoHang is null then '' else lh.MaLoHang end as TenLoHang,
    		CAST(ROUND(3, 0) as float) as TrangThaiMoPhieu,
			ROUND(ISNULL(TonKho,0),2) as TonKho,
			isnull(tblVTtri.TenViTris,'') as ViTriKho
		from 
		(
		--- get ct if has tpdinhluong
		select max(ct.ID) as ID,
			max(ct.SoThuTu) as SoThuTu,
			ct.ID_DonViQuiDoi,
			ct.ID_LoHang,
			@ID_HoaDon as ID_HoaDon,
			sum(ct.SoLuong) as SoLuong, 
			max(ct.DonGia) as DonGia,
			max(ct.DonGia) as GiaVon,
			sum(ct.SoLuong * ct.DonGia) as GiaTriHuy,			
			max(ct.TienChietKhau) as TienChietKhau,
			max(ct.GhiChu) as GhiChu
		from BH_HoaDon_ChiTiet ct
		where ct.ID_HoaDon= @ID_HoaDon		
		and (ct.ChatLieu is null or ct.ChatLieu!='5')
		group by ct.ID_DonViQuiDoi, ct.ID_LoHang		
		)ctxk
		join BH_HoaDon hd on hd.ID= ctxk.ID_HoaDon
		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
		join DonViQuiDoi dvqd on ctxk.ID_DonViQuiDoi = dvqd.ID
		join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
		left join DM_LoHang lh on ctxk.ID_LoHang = lh.ID
		left join DM_HangHoa_TonKho tk on (dvqd.ID = tk.ID_DonViQuyDoi and (lh.ID = tk.ID_LoHang or lh.ID is null) and  tk.ID_DonVi = @ID_DonVi)
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
		) tblVTtri on hh.ID = tblVTtri.ID
		where (hh.LaHangHoa = 1 and tk.TonKho is not null)
		end
	else
	begin
		if @loaiHD in (1,3,2,25)
		begin
			---- get soluong sudung GDV (đến thời điểm hiện tại) của các dịch vụ thuộc HD---
			---- get all ID_ChiTietGoiDV (tức ID cthd cua GDV mua) ---
			declare @tblCT_GDVmua table (ID_ChiTietGoiDV uniqueidentifier primary key)
			insert into @tblCT_GDVmua
			select distinct ID_ChiTietGoiDV
			from BH_HoaDon_ChiTiet 
			where ID_HoaDon= @ID_HoaDon and ID_ChiTietGoiDV is not null

			select 
				 ctm.SoLuong as SoLuongMua,
				 ctsd.ID_ChiTietGoiDV,
				 ctsd.SoLuongSuDung,
				 ctsd.SoLuongTra
			into #tblSDDV
			from
			(
			select ct.ID_ChiTietGoiDV, 
				sum(iif(hd.LoaiHoaDon=6, ctsd.SoLuong,0)) as SoLuongTra,
				sum(iif(hd.LoaiHoaDon!=6, ctsd.SoLuong,0)) as SoLuongSuDung
			from @tblCT_GDVmua ct
			join BH_HoaDon_ChiTiet ctsd on ct.ID_ChiTietGoiDV = ctsd.ID_ChiTietGoiDV
			join BH_HoaDon hd on ctsd.ID_HoaDon= hd.ID
			where hd.ChoThanhToan= 0
			and hd.LoaiHoaDon in ( 1,25,6)
			AND (ctsd.ID_ChiTietDinhLuong IS NULL OR ctsd.ID_ChiTietDinhLuong = ctsd.ID) --- khong get tpdinhluong khi sudung GDV
			group by ct.ID_ChiTietGoiDV
			) ctsd
			join BH_HoaDon_ChiTiet ctm  on ctm.ID= ctsd.ID_ChiTietGoiDV	


			------- cthxuatkho -----
				declare @tblCTXuatKho table (SoLuongXuat float, ID_ChiTietGoiDV uniqueidentifier)
				insert into @tblCTXuatKho
				select 
					SUM(ctxk.SoLuong) as SoLuongXuat, ctxk.ID_ChiTietGoiDV
				from BH_HoaDon  hdxk
				join BH_HoaDon_ChiTiet ctxk on ctxk.ID_HoaDon = hdxk.ID
				where hdxk.ID_HoaDon = @ID_HoaDon
				and LoaiHoaDon = 8 and ChoThanhToan='0'
				group by ctxk.ID_ChiTietGoiDV

					select 
						ct.ID,ct.ID_HoaDon,DonGia,ct.GiaVon,SoLuong,ThanhTien,ThanhToan,ct.ID_DonViQuiDoi, ct.ID_ChiTietDinhLuong, ct.ID_ChiTietGoiDV,
    							ct.TienChietKhau AS GiamGia,PTChietKhau,ct.GhiChu,ct.TienChietKhau,
    							(ct.DonGia - ct.TienChietKhau) as GiaBan,
								qd.GiaBan as GiaBanHH, ---- used to nhaphang from hoadon
    							CAST(SoThuTu AS float) AS SoThuTu,ct.ID_KhuyenMai, ISNULL(ct.TangKem,'0') as TangKem, ct.ID_TangKem,
								-- replace char enter --> char space
    							(REPLACE(REPLACE(TenHangHoa,CHAR(13),''),CHAR(10),'') +
    							CASE WHEN (qd.ThuocTinhGiaTri is null or qd.ThuocTinhGiaTri = '') then '' else '_' + qd.ThuocTinhGiaTri end +
    							CASE WHEN TenDonVitinh = '' or TenDonViTinh is null then '' else ' (' + TenDonViTinh + ')' end +
    							CASE WHEN MaLoHang is null then '' else '. Lô: ' + MaLoHang end) as TenHangHoaFull,
    				
    							hh.ID AS ID_HangHoa,
								hh.ID_Xe,
								hh.LaHangHoa,
								hh.QuanLyTheoLoHang,
								hh.TenHangHoa, 
								isnull(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
								ISNULL(nhh.TenNhomHangHoa,'') as TenNhomHangHoa,
								ISNULL(ID_NhomHang,'00000000-0000-0000-0000-000000000000') as ID_NhomHangHoa,	
    							TenDonViTinh,MaHangHoa,
    							lo.ID AS ID_LoHang,
								ISNULL(MaLoHang,'') as MaLoHang,
								lo.NgaySanXuat, lo.NgayHetHan,
								qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
								ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
								CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
								CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach,
								CAST(ISNULL(ct.PTThue,0) as float) as PTThue,
								CAST(ISNULL(ct.TienThue,0) as float) as TienThue,
								CAST(ISNULL(ct.ThoiGianBaoHanh,0) as float) as ThoiGianBaoHanh,
								CAST(ISNULL(ct.LoaiThoiGianBH,0) as float) as LoaiThoiGianBH,
								Case when hh.LaHangHoa='1' then 0 else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end PhiDichVu,
								Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end LaPTPhiDichVu,
								CAST(0 as float) as TongPhiDichVu, -- set default PhiDichVu = 0 (caculator again .js)
								CAST(ISNULL(ct.Bep_SoLuongYeuCau,0) as float) as Bep_SoLuongYeuCau,
								CAST(ISNULL(ct.Bep_SoLuongHoanThanh,0) as float) as Bep_SoLuongHoanThanh, -- view in CTHD NhaHang
								CAST(ISNULL(ct.Bep_SoLuongChoCungUng,0) as float) as Bep_SoLuongChoCungUng,
								ISNULL(hh.SoPhutThucHien,0) as SoPhutThucHien, -- lay so phut theo cai dat
								ISNULL(ct.ThoiGianThucHien,0)  as ThoiGianThucHien,-- sophut thuc te thuchien	
								ISNULL(ct.QuaThoiGian,0)  as QuaThoiGian,
				
								case when hh.LaHangHoa='0' then 0 else ISNULL(tk.TonKho,0) end as TonKho,
								ct.ID_ViTri,
								ISNULL(vt.TenViTri,'') as TenViTri,			
								ThoiGian,ct.ThoiGianHoanThanh, ISNULL(hh.GhiChu,'') as GhiChuHH,
								ISNULL(ct.DiemKhuyenMai,0) as DiemKhuyenMai,
								ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
								ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
								ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
								ISNULL(hh.ChietKhauMD_NVTheoPT,'0') as ChietKhauMD_NVTheoPT,
								ct.ChatLieu,
								isnull(ct.DonGiaBaoHiem,0) as DonGiaBaoHiem,
								iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='',hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe,					
								ct.ID_LichBaoDuong,
								iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
								ct.ID_ParentCombo,
								qd.GiaNhap,
								isnull(hdXK.SoLuongXuat,0) as SoLuongXuat,
								isnull(hdmua.SoLuongMua,0) as SoLuongMua,
								isnull(hdmua.SoLuongMua,0) - isnull(hdmua.SoLuongSuDung,0) - isnull(hdmua.SoLuongTra,0) as SoLuongDVConLai,
								isnull(hdmua.SoLuongSuDung,0) as SoLuongDVDaSuDung,
								isnull(tblVTtri.TenViTris,'') as ViTriKho
					from
					(
					select ct.*
					from BH_HoaDon_ChiTiet ct
					where ct.ID_HoaDon= @ID_HoaDon
					and (ct.ChatLieu is null or ct.ChatLieu!='5')
					AND (ct.ID_ChiTietDinhLuong IS NULL OR ct.ID_ChiTietDinhLuong = ct.ID)
					and (ct.ID_ParentCombo IS NULL OR ct.ID_ParentCombo = ct.ID)
					) ct
					JOIN DonViQuiDoi qd ON ct.ID_DonViQuiDoi = qd.ID
    				JOIN DM_HangHoa hh ON qd.ID_HangHoa= hh.ID    		
    				left JOIN DM_NhomHangHoa nhh ON hh.ID_NhomHang= nhh.ID    							
    				LEFT JOIN DM_LoHang lo ON ct.ID_LoHang = lo.ID
					left join DM_HangHoa_TonKho tk on ct.ID_DonViQuiDoi= tk.ID_DonViQuyDoi 
						and tk.ID_DonVi= @ID_DonVi and  (ct.ID_LoHang= tk.ID_LoHang OR (ct.ID_LoHang is null and tk.ID_LoHang is null)) 
					left join DM_ViTri vt on ct.ID_ViTri= vt.ID
					left join #tblSDDV hdmua  on ct.ID_ChiTietGoiDV = hdmua.ID_ChiTietGoiDV				
					left join @tblCTXuatKho hdXK on ct.ID = hdXK.ID_ChiTietGoiDV 
					
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
						) tblVTtri on hh.ID = tblVTtri.ID
						order by ct.SoThuTu

				
		end
		else
			begin
				SELECT 
    			cthd.ID,
				cthd.ID_HoaDon, 
				cthd.ID_ParentCombo,
				cthd.ID_ChiTietDinhLuong,
				cthd.DonGia, 
				cthd.GiaVon, 
				cast(cthd.SoThuTu as float) as SoThuTu,
				SoLuong, 
				cthd.ThanhTien, 
				TienChietKhau, 
				cthd.ThanhToan, 
				cthd.TienThue, 
				isnull(cthd.PTThue,0) as PTThue, 
				dvqd.ID as ID_DonViQuiDoi,
    			dvqd.ID_HangHoa, dvqd.TenDonViTinh, dvqd.MaHangHoa,
				TienChietKhau as GiamGia, PTChietKhau,
				(cthd.DonGia - cthd.TienChietKhau) as GiaBan,
				hd.NgayLapHoaDon as ThoiGian, cthd.GhiChu,
    			cthd.ID_KhuyenMai,			
				lo.NgaySanXuat, lo.NgayHetHan,
    			dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				concat(TenHangHoa ,
    			dvqd.ThuocTinhGiaTri, 
    			Case when dvqd.TenDonVitinh = '' or dvqd.TenDonViTinh is null then '' else ' (' + dvqd.TenDonViTinh + ')' end +
    			Case when lo.MaLoHang is null then '' else '. Lô: ' + lo.MaLoHang end) as TenHangHoaFull,
    			LaHangHoa, QuanLyTheoLoHang,
				TenHangHoa,		
				hh.TenHangHoa as TenHangHoaThayThe,
    			TyLeChuyenDoi, YeuCau,
    			lo.ID AS ID_LoHang, ISNULL(MaLoHang,'') as MaLoHang, 			
				ISNULL(hhtonkho.TonKho, 0) as TonKho, 
				hd.ID_DonVi, dvqd.GiaNhap, 
				dvqd.GiaBan as GiaBanMaVach, hh.ID_NhomHang as ID_NhomHangHoa,
				dvqd.LaDonViChuan, hh.ChiPhiThucHien as PhiDichVu, cast(ISNULL(hh.QuyCach,1) as float) as QuyCach, 
				Case when hh.LaHangHoa='1' then '0' else ISNULL(hh.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
				dvqd.GiaBan, dvqd.GiaBan as GiaBanHH, -- use to get banggiachung  of cthd (at NhapHangChiTiet),
				ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
				ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
				ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
				ISNULL(hh.ChietKhauMD_NVTheoPT,'0') as ChietKhauMD_NVTheoPT,
				ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
				cthd.ID_LichBaoDuong,
				iif(hh.LoaiHangHoa is null or hh.LoaiHangHoa= 0, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
				isnull(tblVTtri.TenViTris,'') as ViTriKho
    			FROM BH_HoaDon hd
    			JOIN BH_HoaDon_ChiTiet cthd ON hd.ID= cthd.ID_HoaDon
    			JOIN DonViQuiDoi dvqd ON cthd.ID_DonViQuiDoi = dvqd.ID
    			JOIN DM_HangHoa hh ON dvqd.ID_HangHoa= hh.ID
    			LEFT JOIN DM_LoHang lo ON cthd.ID_LoHang = lo.ID
				LEFT JOIN DM_HangHoa_TonKho hhtonkho on dvqd.ID = hhtonkho.ID_DonViQuyDoi
				and (hhtonkho.ID_LoHang = cthd.ID_LoHang or cthd.ID_LoHang is null) and hhtonkho.ID_DonVi = @ID_DonVi
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
				) tblVTtri on hh.ID = tblVTtri.ID
    			WHERE cthd.ID_HoaDon = @ID_HoaDon 
				and (cthd.ID_ChiTietDinhLuong = cthd.ID or cthd.ID_ChiTietDinhLuong is null)
				and (cthd.ID_ParentCombo IS NULL OR cthd.ID_ParentCombo = cthd.ID)
				and (cthd.ChatLieu is null or cthd.ChatLieu!='5')
				order by cthd.SoThuTu desc
			end
	end

    END");

			Sql(@"ALTER PROCEDURE [dbo].[GetList_GoiDichVu_afterUseAndTra]
 --declare 
 @IDChiNhanhs nvarchar(max) = 'd93b17ea-89b9-4ecf-b242-d03b8cde71de',
   @IDNhanViens nvarchar(max) = null,
   @DateFrom datetime = '2022-01-13',
   @DateTo datetime = null,
   @TextSearch nvarchar(max) = 'GDV-0009',
   @CurrentPage int =0,
   @PageSize int = 10
AS
BEGIN

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
				tblConLai.SoLuongDung,
				tblConLai.SoLuongConLai
			FROM
			(
					select 
						tblUnion.ID,
						sum(tblUnion.SoLuongBan) as SoLuongBan,
						sum(isnull(tblUnion.SoLuongTra,0)) as SoLuongTra,
						sum(isnull(tblUnion.SoLuongDung,0)) as SoLuongDung,
						sum(tblUnion.SoLuongBan) - sum(isnull(tblUnion.SoLuongDung,0)) - sum(isnull(tblUnion.SoLuongTra,0)) as SoLuongConLai
					from
					(
							------ mua ----
							select 
								hd.ID,
								sum(ct.SoLuong) as SoLuongBan,
								0 as SoLuongTra,
								0 as SoLuongDung
							from BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
							where hd.ChoThanhToan=0
							and hd.LoaiHoaDon = 19 ---- khong trahang HDSC
							and hd.NgayLapHoaDon between @DateFrom and @DateTo	
							and (@IDChiNhanhs ='' or exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID))
							and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID) ---- chi get hanghoa + dv
							and (ct.ID_ParentCombo is null OR ct.ID_ParentCombo != ct.ID) ---- khong get parent, get TP combo
							group by hd.ID


							union all

							----- tra ----
							select hd.ID_HoaDon,
								0 as SoLuongBan,
								sum(ct.SoLuong) as SoLuongTra,
								0 as SoLuongDung
							from BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon  
							where hd.ChoThanhToan = 0  
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID)													
							group by hd.ID_HoaDon

							union all
							----- sudung ----
								select hd.ID_HoaDon,
								0 as SoLuongBan,
								0 as SoLuongTra,
								sum(ct.SoLuong) as SoLuongDung
							from BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon  
							where hd.ChoThanhToan = 0  
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID)													
							group by hd.ID_HoaDon
		
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
			nv.TenNhanVien,	
				--iif (cnLast.LoaiHoaDonGoc != 6, cnLast.TongTienHDTra,
				--	iif(cnLast.TongGiaTriTra > cnLast.ConNo1, cnLast.TongTienHDTra + cnLast.ConNo1,cnLast.TongTienHDTra)) as TongTienHDTra,
					
				iif (cnLast.LoaiHoaDonGoc != 6, cnLast.ConNo1,
					iif(cnLast.TongGiaTriTra > cnLast.ConNo1, 0, cnLast.ConNo1)) as ConNo
		from
		(
		select 
				tblLast.*,
				tblLast.TongThanhToan 
					--- neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = 0
					- iif(tblLast.LoaiHoaDonGoc = 6, iif(tblLast.LuyKeTraHang > 0, 0, abs(tblLast.LuyKeTraHang)), tblLast.LuyKeTraHang)
					- tblLast.KhachDaTra - isnull(thuBH.BaoHiemDaTra,0) as ConNo1 ---- ConNo = TongThanhToan - GtriBuTru
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

			Sql(@"ALTER PROCEDURE [dbo].[GetList_GoiDichVu_Where]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @maHD [nvarchar](max),
	@ID_NhanVienLogin nvarchar(max) = '',
	@NguoiTao nvarchar(max)='',
	@IDViTris nvarchar(max)='',
	@IDBangGias nvarchar(max)='',
	@TrangThai nvarchar(max)='0,1,2',
	@PhuongThucThanhToan nvarchar(max)='',
	@ColumnSort varchar(max)='NgayLapHoaDon',
	@SortBy varchar(max)= 'DESC',
	@CurrentPage int,
	@PageSize int
AS
BEGIN
	set nocount on;

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);

	 declare @tblNhanVien table (ID uniqueidentifier)
	 if isnull(@ID_NhanVienLogin,'') !=''
		begin
			insert into @tblNhanVien
			select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanh,'HoaDon_XemDS_PhongBan','HoaDon_XemDS_HeThong');
		end

	declare @tblChiNhanh table (ID uniqueidentifier)
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@ID_ChiNhanh);

	declare @tblPhuongThuc table (PhuongThuc int)
	insert into @tblPhuongThuc
	select Name from dbo.splitstring(@PhuongThucThanhToan)
	

	declare @tblTrangThai table (TrangThaiHD tinyint primary key)
	insert into @tblTrangThai
	select Name from dbo.splitstring(@TrangThai);


	declare @tblViTri table (ID varchar(40))
	insert into @tblViTri
	select Name from dbo.splitstring(@IDViTris) where Name!=''

	declare @tblBangGia table (ID varchar(40))
	insert into @tblBangGia
	select Name from dbo.splitstring(@IDBangGias) where Name!=''
	
	if @timeStart='2016-01-01'		
		select @timeStart = min(NgayLapHoaDon) from BH_HoaDon where LoaiHoaDon=19
	;with data_cte
	as
	(
    SELECT 
    	c.ID,
    	c.ID_BangGia,
    	c.ID_HoaDon,
    	c.ID_ViTri,
    	c.ID_NhanVien,
    	c.ID_DoiTuong,
		c.ID_Xe,
		xe.BienSo,
		c.ID_PhieuTiepNhan,
    	c.TheoDoi,
    	c.ID_DonVi,
    	c.ID_KhuyenMai,
    	c.ChoThanhToan,
    	c.MaHoaDon,  	
    	c.NgayLapHoaDon,
		ISNULL(c.MaDoiTuong,'') as MaDoiTuong,
    	c.TenDoiTuong,
    	c.Email,
    	c.DienThoai,
    	c.NguoiTaoHD,
    	c.DiaChiKhachHang,
    	c.KhuVuc,
    	c.PhuongXa,
    	c.TenDonVi,
    	c.TenNhanVien,
    	c.DienGiai,
    	c.TenBangGia,
    	c.TenPhongBan,
    	c.TongTienHang,
		c.TongGiamGia, 
		--c.TongThanhToan,
		c.PhaiThanhToan,		
		c.ThuTuThe, c.TienMat, c.TienATM,c.TienDoiDiem, c.ChuyenKhoan, c.KhachDaTra,c.TongChietKhau,c.TongTienThue,PTThueHoaDon,
		c.TongThueKhachHang,
		ID_TaiKhoanPos,
		ID_TaiKhoanChuyenKhoan,
    	c.TrangThai,
    	c.KhuyenMai_GhiChu,
    	c.KhuyeMai_GiamGia,
		c.LoaiHoaDonGoc,
		c.TongGiaTriTra,
    	iif(c.TongThanhToan1 =0 and c.PhaiThanhToan> 0, c.PhaiThanhToan, c.TongThanhToan1) as TongThanhToan,
				isnull(iif(c.ID_HoaDon is null,
					iif(c.KhachNo <= 0, 0, ---  khachtra thuatien --> công nợ âm
						case when c.TongGiaTriTra > c.KhachNo then c.KhachNo						
						else c.TongGiaTriTra end),
					(select dbo.BuTruTraHang_HDDoi(ID_HoaDon,NgayLapHoaDon,ID_HoaDonGoc, LoaiHoaDonGoc))				
				),0) as LuyKeTraHang,
    	c.LoaiHoaDon,
    	c.DiaChiChiNhanh,
    	c.DienThoaiChiNhanh,
    	c.DiemGiaoDich,
    	c.DiemSauGD, -- add 02.08.2018 (bind InHoaDon)
    	c.HoaDon_HangHoa, -- string contail all MaHangHoa,TenHangHoa of HoaDon
    	CONVERT(nvarchar(10),c.NgayApDungGoiDV,103) as NgayApDungGoiDV,
    	CONVERT(nvarchar(10),c.HanSuDungGoiDV,103) as HanSuDungGoiDV
		
    	FROM
    	(
    		select 
    		a.ID as ID,
    		hdXMLOut.HoaDon_HangHoa,
    		bhhd.ID_DoiTuong,
    			-- Neu theo doi = null --> kiem tra neu la khach le --> theodoi = true, nguoc lai = 1
    		CASE 
    			WHEN dt.TheoDoi IS NULL THEN 
    				CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    			ELSE dt.TheoDoi
    		END AS TheoDoi,
    		bhhd.ID_HoaDon,
    		bhhd.ID_NhanVien,
    		bhhd.ID_DonVi,
			bhhd.ID_Xe,
			bhhd.ID_PhieuTiepNhan,
    		bhhd.ChoThanhToan,
    		bhhd.ID_KhuyenMai,
    		bhhd.KhuyenMai_GhiChu,
    		bhhd.LoaiHoaDon,
			isnull(bhhd.PTThueHoaDon,0) as  PTThueHoaDon,
    		ISNULL(bhhd.KhuyeMai_GiamGia,0) AS KhuyeMai_GiamGia,
    		ISNULL(bhhd.DiemGiaoDich,0) AS DiemGiaoDich,
    		ISNULL(gb.ID,N'00000000-0000-0000-0000-000000000000') as ID_BangGia,
			ISNULL(vt.ID,N'00000000-0000-0000-0000-000000000000') as ID_ViTri,
    		ISNULL(vt.TenViTri,'') as TenPhongBan,
    		bhhd.MaHoaDon,   		
    		bhhd.NgayLapHoaDon,
    		bhhd.NgayApDungGoiDV,
    		bhhd.HanSuDungGoiDV,
			dt.MaDoiTuong,
			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
    		ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau,
			ISNULL(dt.TenDoiTuong_ChuCaiDau, N'kl') as TenDoiTuong_ChuCaiDau,
			ISNULL(dt.Email, N'') as Email,
			ISNULL(dt.DienThoai, N'') as DienThoai,
			ISNULL(dt.DiaChi, N'') as DiaChiKhachHang,
			ISNULL(tt.TenTinhThanh, N'') as KhuVuc,
			ISNULL(qh.TenQuanHuyen, N'') as PhuongXa,
			ISNULL(dv.TenDonVi, N'') as TenDonVi,
			ISNULL(dv.DiaChi, N'') as DiaChiChiNhanh,
			ISNULL(dv.SoDienThoai, N'') as DienThoaiChiNhanh,
			ISNULL(nv.TenNhanVien, N'') as TenNhanVien,
			ISNULL(nv.TenNhanVienKhongDau, N'') as TenNhanVienKhongDau,
    		ISNULL(dt.TongTichDiem,0) AS DiemSauGD,
    		ISNULL(gb.TenGiaBan,N'Bảng giá chung') AS TenBangGia,
    		bhhd.DienGiai,
    		bhhd.NguoiTao as NguoiTaoHD,
    		bhhd.TongChietKhau,
			bhhd.TongThanhToan as TongThanhToan1,
			ISNULL(bhhd.TongThueKhachHang,0) as TongThueKhachHang,
			ISNULL(bhhd.TongTienThue,0) as TongTienThue,
			bhhd.TongTienHang,
			bhhd.TongGiamGia,
			bhhd.PhaiThanhToan,

			hdgoc.ID_HoaDon as ID_HoaDonGoc,
			isnull(hdgoc.LoaiHoaDon,0) as LoaiHoaDonGoc,
			hdgoc.MaHoaDon as MaHoaDonGoc,

			ISNULL(allTra.TongGtriTra,0) as TongGiaTriTra,
			ISNULL(allTra.NoTraHang,0) as NoTraHang,

    		a.ThuTuThe,
    		a.TienMat,
			a.TienATM,
			a.TienDoiDiem,
    		a.ChuyenKhoan,
    		a.KhachDaTra,
			ID_TaiKhoanPos,
			ID_TaiKhoanChuyenKhoan,

			ISNULL(bhhd.PhaiThanhToan,0) - ISNULL(a.KhachDaTra,0) as KhachNo,
    		
			case bhhd.ChoThanhToan
				when 1 then '1'
				when 0 then '0'
			else '4' end as TrangThaiHD,
    		Case When bhhd.ChoThanhToan = '1' then N'Phiếu tạm' when bhhd.ChoThanhToan = '0' then N'Hoàn thành' else N'Đã hủy' end as TrangThai,
			case when a.TienMat > 0 then
				case when a.TienATM > 0 then	
					case when a.ChuyenKhoan > 0 then
						case when a.ThuTuThe > 0 then '1,2,3,4' else '1,2,3' end												
						else 
							case when a.ThuTuThe > 0 then  '1,2,4' else '1,2' end end
						else
							case when a.ChuyenKhoan > 0 then 
								case when a.ThuTuThe > 0 then '1,3,4' else '1,3' end
								else 
										case when a.ThuTuThe > 0 then '1,4' else '1' end end end
				else
					case when a.TienATM > 0 then
						case when a.ChuyenKhoan > 0 then
								case when a.ThuTuThe > 0 then '2,3,4' else '2,3' end	
								else 
									case when a.ThuTuThe > 0 then '2,4' else '2' end end
							else 		
								case when a.ChuyenKhoan > 0 then
									case when a.ThuTuThe > 0 then '3,4' else '3' end
									else 
									case when a.ThuTuThe > 0 then '4' else '5' end end end end
									
						as PTThanhToan
    		FROM
    		(
    			Select 
    			b.ID,
    			SUM(ISNULL(b.ThuTuThe, 0)) as ThuTuThe,
    			SUM(ISNULL(b.TienMat, 0)) as TienMat,
				SUM(ISNULL(b.TienATM, 0)) as TienATM,
    			SUM(ISNULL(b.TienCK, 0)) as ChuyenKhoan,
				SUM(ISNULL(b.TienDoiDiem, 0)) as TienDoiDiem,
    			SUM(ISNULL(b.TienThu, 0)) as KhachDaTra,
				max(b.ID_TaiKhoanPos) as ID_TaiKhoanPos,
				max(b.ID_TaiKhoanChuyenKhoan) as ID_TaiKhoanChuyenKhoan
    			from
    			(
    				Select 
    				bhhd.ID,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.TienMat, 0) else ISNULL(hdct.TienMat, 0) * (-1) end end as TienMat,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = '11' then case when TaiKhoanPOS = 1 then ISNULL(hdct.TienGui, 0) else 0 end else ISNULL(hdct.TienGui, 0) * (-1) end end as TienATM,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = '11' then case when TaiKhoanPOS = 0 then ISNULL(hdct.TienGui, 0) else 0 end else ISNULL(hdct.TienGui, 0) * (-1) end end as TienCK,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.ThuTuThe, 0) else ISNULL(hdct.ThuTuThe, 0) * (-1) end end as ThuTuThe,
					case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = 11 then 
							case when ISNULL(hdct.DiemThanhToan, 0) = 0 then 0 else ISNULL(hdct.Tienthu, 0) end
							else case when ISNULL(hdct.DiemThanhToan, 0) = 0 then 0 else -ISNULL(hdct.Tienthu, 0) end end end as TienDoiDiem,
    				Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = '11' then ISNULL(hdct.Tienthu, 0) else ISNULL(hdct.Tienthu, 0) * (-1) end end as TienThu,
					case when qhd.TrangThai = 0 then '00000000-0000-0000-0000-000000000000' else case when TaiKhoanPOS = 1 then hdct.ID_TaiKhoanNganHang else '00000000-0000-0000-0000-000000000000' end end as ID_TaiKhoanPos,
					case when qhd.TrangThai = 0 then '00000000-0000-0000-0000-000000000000' else case when TaiKhoanPOS = 0 then hdct.ID_TaiKhoanNganHang else '00000000-0000-0000-0000-000000000000' end end as ID_TaiKhoanChuyenKhoan
    				from BH_HoaDon bhhd
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan	
    				left join Quy_HoaDon qhd on hdct.ID_HoaDon = qhd.ID  
					left join DM_TaiKhoanNganHang tk on tk.ID= hdct.ID_TaiKhoanNganHang		
    				where bhhd.LoaiHoaDon = '19' and bhhd.NgayLapHoadon between @timeStart and @timeEnd
					and bhhd.ID_DonVi IN (Select * from splitstring(@ID_ChiNhanh))    
					and (isnull(@ID_NhanVienLogin,'')='' or exists( select * from @tblNhanVien nv where nv.ID= bhhd.ID_NhanVien) or bhhd.NguoiTao= @NguoiTao)
    			) b
    			group by b.ID 
    		) as a			
    		join BH_HoaDon bhhd on a.ID = bhhd.ID   	
			left join BH_HoaDon hdgoc on bhhd.ID_HoaDon= hdgoc.ID
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
			) allTra on allTra.ID_HoaDon = bhhd.ID
    		left join DM_DoiTuong dt on bhhd.ID_DoiTuong = dt.ID
    		left join DM_DonVi dv on bhhd.ID_DonVi = dv.ID
    		left join NS_NhanVien nv on bhhd.ID_NhanVien = nv.ID 
    		left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    		left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    		left join DM_GiaBan gb on bhhd.ID_BangGia = gb.ID
    		left join DM_ViTri vt on bhhd.ID_ViTri = vt.ID			
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
			left join Gara_DanhMucXe xe on c.ID_Xe= xe.ID
			where (@IDViTris ='' or exists (select ID from @tblViTri vt2 where vt2.ID= c.ID_ViTri))
			and (@IDBangGias ='' or exists (select ID from @tblBangGia bg where bg.ID= c.ID_BangGia))
			and exists (select TrangThaiHD from @tblTrangThai tt where c.TrangThaiHD= tt.TrangThaiHD)
		    and (@PhuongThucThanhToan ='' or exists(SELECT Name FROM splitstring(c.PTThanhToan) pt join @tblPhuongThuc pt2 on pt.Name = pt2.PhuongThuc))
			and	((select count(Name) from @tblSearch b where     			
				c.MaHoaDon like '%'+b.Name+'%'
				or c.NguoiTaoHD like '%'+b.Name+'%'				
				or c.TenNhanVien like '%'+b.Name+'%'
				or c.TenNhanVienKhongDau like '%'+b.Name+'%'
				or c.DienGiai like '%'+b.Name+'%'
				or c.MaDoiTuong like '%'+b.Name+'%'		
				or c.TenDoiTuong like '%'+b.Name+'%'
				or c.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or c.DienThoai like '%'+b.Name+'%'						
				or xe.BienSo like '%'+b.Name+'%'	
				or c.HoaDon_HangHoa like '%'+b.Name+'%'			
				)=@count or @count=0)	
				), 
				tblDebit as
				(
				select 
					cnLast.ID,
					---- hdDoi co CongNo  < tongtra --> butru = Luyketrahang + conngno
					iif (cnLast.LoaiHoaDonGoc != 6, cnLast.TongTienHDTra,
						iif(cnLast.TongGiaTriTra > cnLast.ConNo, cnLast.TongTienHDTra + cnLast.ConNo,cnLast.TongTienHDTra)) as TongTienHDTra,
					
					iif (cnLast.LoaiHoaDonGoc != 6, cnLast.ConNo,
						iif(cnLast.TongGiaTriTra > cnLast.ConNo, 0, cnLast.ConNo)) as ConNo
						
				from
				(
					select 
						c.ID,
						c.LoaiHoaDonGoc,
						c.TongGiaTriTra,
						iif(c.LoaiHoaDonGoc = 6, iif(c.LuyKeTraHang > 0, 0, abs(c.LuyKeTraHang)), c.LuyKeTraHang) as TongTienHDTra,
					
						iif(c.ChoThanhToan is null,0, 
							----- hdDoi co congno < tongtra							
							c.TongThanhToan 
								--- neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = 0							
								- iif(c.LoaiHoaDonGoc = 6, iif(c.LuyKeTraHang > 0, 0, abs(c.LuyKeTraHang)), c.LuyKeTraHang)
								- c.KhachDaTra ) as ConNo ---- ConNo = TongThanhToan - GtriBuTru
					from data_cte c
					) cnLast 
				),
			count_cte
		as (
			select count(dt.ID) as TotalRow,
				CEILING(COUNT(dt.ID) / CAST(@PageSize as float ))  as TotalPage,
				sum(TongTienHang) as SumTongTienHang,			
				sum(TongGiamGia) as SumTongGiamGia,
				sum(KhachDaTra) as SumKhachDaTra,								
				sum(KhuyeMai_GiamGia) as SumKhuyeMai_GiamGia,								
				sum(PhaiThanhToan) as SumPhaiThanhToan,				
				sum(TongThanhToan) as SumTongThanhToan,
				sum(TienDoiDiem) as SumTienDoiDiem,
				sum(ThuTuThe) as SumThuTuThe,				
				sum(TienMat) as SumTienMat,
				sum(TienATM) as SumPOS,
				sum(ChuyenKhoan) as SumChuyenKhoan,				
				sum(TongTienThue) as SumTongTienThue,
				sum(ConNo) as SumConNo
			from data_cte dt
			left join tblDebit cn on dt.ID= cn.ID
		)
		select dt.*, cte.*, cn.ConNo, cn.TongTienHDTra	
		from data_cte dt
		left join tblDebit cn on dt.ID= cn.ID
		cross join count_cte cte	
		order by 
			case when @SortBy <> 'ASC' then 0
			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end ASC,
			case when @SortBy <> 'DESC' then 0
			when @ColumnSort='NgayLapHoaDon' then NgayLapHoaDon end DESC,
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
			when @ColumnSort='KhachCanTra' then PhaiThanhToan end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='KhachCanTra' then PhaiThanhToan end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='KhachDaTra' then KhachDaTra end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='KhachDaTra' then KhachDaTra end DESC	
				
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY
    	
END");

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetInforKhachHang_ByID]
--declare   
@ID_DoiTuong uniqueidentifier ='FF90A012-01C3-4399-9673-00003F378D59',
    @ID_ChiNhanh [nvarchar](max)='172273FC-1ABB-46F5-8356-B36F89D3652A',
    @timeStart [nvarchar](max)='2023-03-13',
    @timeEnd [nvarchar](max)='2023-03-13'
AS
BEGIN
	SET NOCOUNT ON;
	declare @LoaiDoiTuong int
	select @LoaiDoiTuong= LoaiDoiTuong from DM_DoiTuong where ID = @ID_DoiTuong

    SELECT 
    			  dt.ID as ID,
    			  dt.MaDoiTuong, 
				  dt.LoaiDoiTuong, 
    			  case when dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else  ISNULL(dt.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as ID_NhomDoiTuong,
    			  dt.TenDoiTuong,
    			  dt.TenDoiTuong_KhongDau,
    			  dt.TenDoiTuong_ChuCaiDau,
    			  dt.ID_TrangThai,
    			  dt.GioiTinhNam,
    			  dt.NgaySinh_NgayTLap,
    			  ISNULL(dt.DienThoai,'') as DienThoai,
    			  ISNULL(dt.Email,'') as Email,
    			  ISNULL(dt.DiaChi,'') as DiaChi,
    			  ISNULL(dt.MaSoThue,'') as MaSoThue,
    			  ISNULL(dt.GhiChu,'') as GhiChu,
				  dt.TaiKhoanNganHang,
    			  dt.NgayTao,
    			  dt.DinhDang_NgaySinh,
    			  ISNULL(dt.NguoiTao,'') as NguoiTao,
    			  dt.ID_NguonKhach,
    			  dt.ID_NhanVienPhuTrach,
    			  dt.ID_NguoiGioiThieu,
    			  dt.ID_DonVi,
    			  dt.LaCaNhan,
    			  CAST(ISNULL(dt.TongTichDiem,0) as float) as TongTichDiem,
				  dt.TenNhomDoiTuongs as TenNhomDT,    			 
    			  dt.ID_TinhThanh,
    			  dt.ID_QuanHuyen,
				  dt.TheoDoi,
				  dt.NgayGiaoDichGanNhat,
    			  ISNULL(dt.TrangThai_TheGiaTri,1) as TrangThai_TheGiaTri,
    			  CAST(ROUND(ISNULL(a.NoHienTai,0), 0) as float) as NoHienTai,
    			  CAST(ROUND(ISNULL(a.TongBan,0), 0) as float) as TongBan,
    			  CAST(ROUND(ISNULL(a.TongBanTruTraHang,0), 0) as float) as TongBanTruTraHang,
    			  CAST(ROUND(ISNULL(a.TongMua,0), 0) as float) as TongMua,
    			  CAST(ROUND(ISNULL(a.SoLanMuaHang,0), 0) as float) as SoLanMuaHang,
    			  CAST(0 as float) as TongNapThe , 
    			  CAST(0 as float) as SuDungThe , 
    			  CAST(0 as float) as HoanTraTheGiaTri , 
    			  CAST(0 as float) as SoDuTheGiaTri , 
				  ISNULL(dt2.TenDoiTuong,'') as NguoiGioiThieu,
    			  concat(dt.MaDoiTuong,' ',lower(dt.MaDoiTuong) ,' ', dt.TenDoiTuong,' ', dt.DienThoai,' ', dt.TenDoiTuong_KhongDau)  as Name_Phone			
    		  FROM DM_DoiTuong dt
			  left join DM_DoiTuong dt2 on dt.ID_NguoiGioiThieu = dt2.ID
			  LEFT JOIN (
    					SELECT tblThuChi.ID_DoiTuong,   	
						sum(iif(@LoaiDoiTuong=3, ISNULL(tblThuChi.BH_DoanhThu,0),ISNULL(tblThuChi.KH_DoanhThu,0))
							+ ISNULL(tblThuChi.TienChi,0)
							+ ISNULL(tblThuChi.HoanTraSoDuTGT,0)
							- ISNULL(tblThuChi.TienThu,0)
							- ISNULL(tblThuChi.GiaTriTra,0)) as NoHienTai,

							sum(iif(@LoaiDoiTuong=3, ISNULL(tblThuChi.BH_DoanhThu, 0),ISNULL(tblThuChi.KH_DoanhThu, 0))) as TongBan,
							sum(ISNULL(tblThuChi.ThuTuThe,0)) as ThuTuThe,
							sum(iif(@LoaiDoiTuong=3, ISNULL(tblThuChi.BH_DoanhThu, 0),ISNULL(tblThuChi.KH_DoanhThu, 0)) 
								- ISNULL(tblThuChi.GiaTriTra,0)) AS TongBanTruTraHang,
							sum(ISNULL(tblThuChi.GiaTriTra, 0) 
								- iif(@LoaiDoiTuong=3, ISNULL(tblThuChi.BH_DoanhThu, 0),ISNULL(tblThuChi.KH_DoanhThu, 0))
								) AS TongMua,
							SUM(ISNULL(tblThuChi.SoLanMuaHang, 0)) as SoLanMuaHang
							
    	
    					FROM
    					(
					
							SELECT 
    							bhd.ID_DoiTuong,
    							0 AS GiaTriTra,    							
								bhd.PhaiThanhToan as KH_DoanhThu,
								0 as BH_DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as HoanTraSoDuTGT
    						FROM BH_HoaDon bhd
    						WHERE bhd.LoaiHoaDon in (1,7,19,22, 25) AND bhd.ChoThanhToan = 0 
							and bhd.ID_DoiTuong = @ID_DoiTuong

								union all

							SELECT 
    							bhd.ID_BaoHiem as ID_DoiTuong,
    							0 AS GiaTriTra,    							
								0 as KH_DoanhThu,
								isnull(bhd.PhaiThanhToanBaoHiem,0) as BH_DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as HoanTraSoDuTGT
    						FROM BH_HoaDon bhd
    						WHERE bhd.LoaiHoaDon in (1,7,19,22, 25) AND bhd.ChoThanhToan = 0 
							and bhd.ID_BaoHiem = @ID_DoiTuong

						
    						
    						 union all
    							-- gia tri trả từ bán hàng
    						SELECT bhd.ID_DoiTuong,    							
								iif(@LoaiDoiTuong=1,bhd.PhaiThanhToan,0)  AS GiaTriTra,
    							0 AS DoanhThu,
								0 as BH_DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as HoanTraSoDuTGT
    						FROM BH_HoaDon bhd   						
    						WHERE (bhd.LoaiHoaDon = 6 OR bhd.LoaiHoaDon = 4) 
							AND bhd.ChoThanhToan = 0 
							and bhd.ID_DoiTuong = @ID_DoiTuong						
    							
    						union all
    
    							-- tienthu
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
								0 as BH_DoanhThu,
    							ISNULL(qhdct.TienThu,0) AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as HoanTraSoDuTGT
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon    						
    						WHERE qhd.LoaiHoaDon = 11 
							and (qhd.TrangThai != 0 OR qhd.TrangThai is null)
							and (qhdct.LoaiThanhToan is null or qhdct.LoaiThanhToan != 3)
							and qhdct.ID_DoiTuong = @ID_DoiTuong
							and qhdct.HinhThucThanhToan!=6

    							
    						 union all    
    							-- tienchi
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
								0 as BH_DoanhThu,
    							0 AS TienThu,
    							ISNULL(qhdct.TienThu,0) AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as HoanTraSoDuTGT
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    						WHERE qhd.LoaiHoaDon = 12 
							AND (qhd.TrangThai != 0 OR qhd.TrangThai is null)
							and (qhdct.LoaiThanhToan is null or qhdct.LoaiThanhToan != 3)
							and qhdct.HinhThucThanhToan!=6
							and qhdct.ID_DoiTuong = @ID_DoiTuong

							union all
							---- hoantra sodu TGT cho khach (giam sodu TGT)
						SELECT 
    							bhd.ID_DoiTuong,    	
								0 AS GiaTriTra,
    							0 AS DoanhThu,
								0 as BH_DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								-sum(bhd.PhaiThanhToan) as HoanTraSoDuTGT
    					FROM BH_HoaDon bhd
						where bhd.LoaiHoaDon = 32 and bhd.ChoThanhToan = 0 	
						group by bhd.ID_DoiTuong
    				)AS tblThuChi GROUP BY tblThuChi.ID_DoiTuong   					
    		) a on dt.ID = a.ID_DoiTuong
			where dt.ID= @ID_DoiTuong
END");

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
				concat(b.TenHangHoa, b.ThuocTinh_GiaTri) as TenHangHoaFull
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
						ctm.ThanhTien,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
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
								ct.ThanhTien as GiaTriTra,
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_TonChuaSuDung]
--declare  
@Text_Search [nvarchar](max) ='xbbh3',
    @MaHH [nvarchar](max) ='',
    @MaKH [nvarchar](max) ='',
    @MaKH_TV [nvarchar](max)='',
    @timeStart [datetime] ='2023-03-10',
    @timeEnd [datetime]='2023-03-10',
    @ID_ChiNhanh [nvarchar](max)='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
    @LaHangHoa [nvarchar](max)='%%',
    @TheoDoi [nvarchar](max)='%1%',
    @TrangThai [nvarchar](max)='%0%',
	@ThoiHan [nvarchar](max)='1',
    @ID_NhomHang [nvarchar](max)='%%',
    @ID_NhomHang_SP [nvarchar](max) =null
AS
BEGIN
	set nocount on;

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table( ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh
	select name from dbo.splitstring(@ID_ChiNhanh)

	declare @dtNow datetime = getdate()

	declare @tblCTMua table(
		MaHoaDon nvarchar(max),
		NgayLapHoaDon datetime,
		NgayApDungGoiDV datetime,
		HanSuDungGoiDV datetime,
		ID_DonVi uniqueidentifier,
		ID_DoiTuong uniqueidentifier,
		ID uniqueidentifier primary key,
		ID_HoaDon uniqueidentifier,
		ID_DonViQuiDoi uniqueidentifier INDEX IX_ID_DonViQuiDoi NONCLUSTERED,
		ID_LoHang uniqueidentifier,
		SoLuong float,
		DonGia float,
		TienChietKhau float,
		ThanhTien float,
		TongTienHang float,
		PTGiamGiaHD float)
	insert into @tblCTMua
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,'2016-01-01',@timeEnd

	select 
		b.MaHangHoa,
		b.TenHangHoa,
		concat(b.TenHangHoa,b.ThuocTinh_GiaTri) as TenHangHoaFull,
		b.TenDonViTinh,		
		b.TenNhomHangHoa as TenNhomHang,
		b.ThuocTinh_GiaTri,
		b.SoLuongBan,
		b.GiaTriBan,
		b.SoLuongTra,
		b.GiaTriTra,
		b.SoLuongSuDung,
		b.GiaTriSuDung,		
		b.SoLuongBan - b.SoLuongTra - b.SoLuongSuDung as SoLuongConLai,
		b.GiaTriBan - b.GiaTriTra - b.GiaTriSuDung as GiaTriConLai
	from
	(
	select 
		qd.MaHangHoa,
		hh.TenHangHoa,
		qd.TenDonViTinh,
		isnull(qd.ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
		nhom.TenNhomHangHoa,
		iif(hh.ID_NhomHang is null, '00000000-0000-0000-0000-000000000000',hh.ID_NhomHang) as ID_NhomHang,
		sum(tbl.SoLuong) as SoLuongBan,
		sum(tbl.ThanhTien) as GiaTriBan,
		sum(tbl.SoLuongTra) as SoLuongTra,
		sum(tbl.GiaTriTra) as GiaTriTra,
		sum(tbl.SoLuongSuDung) as SoLuongSuDung,
		sum(tbl.GiaTriSuDung) as GiaTriSuDung		
	from
	(
		select 
			ctm.ID_DonViQuiDoi,
			ctm.ID_LoHang,
			ctm.ID_DoiTuong,
			ctm.SoLuong,
			ctm.ThanhTien - ctm.ThanhTien* ctm.PTGiamGiaHD as ThanhTien,
			ctm.MaHoaDon,
			isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
			isnull(tbl.GiaTriSuDung,0) as GiaTriSuDung,
			isnull(tbl.SoLuongTra,0) as SoLuongTra,
			isnull(tbl.GiaTriTra,0) as GiaTriTra ,
			iif(ctm.HanSuDungGoiDV is null,1, iif(@dtNow < HanSuDungGoiDV,1,0)) as ThoiHan
		from  @tblCTMua ctm		
		left join (																				
				select 
					ctm.ID as ID_ChiTietGoiDV,
					sum(iif(hd.LoaiHoaDon != 6, ct.SoLuong,0)) as SoLuongSuDung,
					sum(iif(hd.LoaiHoaDon = 6, ct.SoLuong,0)) as SoLuongTra,
					sum(iif(hd.LoaiHoaDon != 6, ct.SoLuong * ((ctm.DonGia - ctm.TienChietKhau) * ( 1 - ctm.PTGiamGiaHD)),0)) as GiaTriSuDung,
					sum(iif(hd.LoaiHoaDon = 6, ct.ThanhTien,0)) as GiaTriTra

				from @tblCTMua ctm
				join BH_HoaDon_ChiTiet ct on ctm.ID= ct.ID_ChiTietGoiDV
				join BH_HoaDon hd on hd.ID = ct.ID_HoaDon
				where hd.ChoThanhToan= 0
				and hd.LoaiHoaDon in (1,6,25)
				and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
				group by ctm.ID							
			) tbl on ctm.ID= tbl.ID_ChiTietGoiDV
		) tbl
		left join DonViQuiDoi qd on tbl.ID_DonViQuiDoi = qd.ID
		left join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
		left join DM_LoHang lo on tbl.ID_LoHang= lo.ID
		left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
		left join DM_DoiTuong dt on tbl.ID_DoiTuong = dt.ID		
		where tbl.ThoiHan like @ThoiHan
		and hh.LaHangHoa like @LaHangHoa		
    	and hh.TheoDoi like @TheoDoi
    	and qd.Xoa like @TrangThai
		AND 
		((select count(Name) 
			from @tblSearchString b where 
			tbl.MaHoaDon like '%'+b.Name+'%'
    		or hh.TenHangHoa like '%'+b.Name+'%'
    		or qd.MaHangHoa like '%'+b.Name+'%'
    		or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
    		or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
			or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    		or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'	
			)=@count or @count=0)
			group by qd.ID, qd.MaHangHoa, hh.TenHangHoa,
				qd.TenDonViTinh,
				qd.ThuocTinhGiaTri,
				nhom.TenNhomHangHoa,
				hh.ID_NhomHang
		) b
		where (b.ID_NhomHang like @ID_NhomHang or b.ID_NhomHang in (select * from splitstring(@ID_NhomHang_SP)))
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoGoiDV_GetCTMua]
    @IDChiNhanhs [nvarchar](max),
    @DateFrom [datetime],
    @DateTo [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    
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
			hd.TongTienHang,
    		Case when hd.TongTienHang = 0 
    		then 0 else (hd.TongGiamGia + hd.KhuyeMai_GiamGia) / hd.TongTienHang end as PTGiamGiaHD
    	from BH_HoaDon hd
    	join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
    	where hd.LoaiHoaDon = 19
    	and hd.ChoThanhToan=0
    	and exists (select cn.ID_DonVi from @tblChiNhanh cn where cn.ID_DonVi= hd.ID_DonVi)
    	and hd.NgayLapHoaDon between @DateFrom and @DateTo
    	and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong= ct.ID)
    	and (ct.ID_ParentCombo is null or ct.ID_ParentCombo!= ct.ID)
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_ChiTiet]
    @Text_Search [nvarchar](max),  
    @MaNCC [nvarchar](max),   
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @ID_NhomNCC [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
set nocount on;
if @MaNCC is null set @MaNCC =''

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);


	   SELECT 
    		a.MaHoaDon,
    		a.NgayLapHoaDon,
    		a.MaNhaCungCap,
    		a.TenNhaCungCap,
    		a.MaHangHoa,
    		a.TenHangHoaFull,
    		a.TenHangHoa,
    		a.ThuocTinh_GiaTri,
    		a.TenDonViTinh,
    		a.TenLoHang,
			a.TienThue,
    		a.SoLuong, 
    		Case When @XemGiaVon = '1' then a.GiaBan else 0 end  as DonGia,
    		Case When @XemGiaVon = '1' then	a.TienChietKhau else 0 end  as TienChietKhau,
    		Case When @XemGiaVon = '1' then a.ThanhTien else 0 end  as ThanhTien,
    		Case When @XemGiaVon = '1' then	a.GiamGiaHD else 0 end  as GiamGiaHD,
    		Case When @XemGiaVon = '1' then a.ThanhTien - a.GiamGiaHD else 0 end as GiaTriNhap, 
    		a.TenNhanVien,
			a.GhiChu
    	FROM
    	(
    		Select hd.MaHoaDon,
    		hd.NgayLapHoaDon,
    		Case When dtn.ID_NhomDoiTuong is null then '30000000-0000-0000-0000-000000000003' else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    		isnull(dt.MaDoiTuong,'NCCL001') as MaNhaCungCap,
    		isnull(dt.TenDoiTuong,N'Nhà cung cấp lẻ') as TenNhaCungCap,
			isnull(dt.TenDoiTuong_KhongDau,N'nha cung cap le') as TenDoiTuong_KhongDau,
			isnull(dt.DienThoai,'') as DienThoai,
    		dvqd.MaHangHoa,
			isnull(hh.TenHangHoa_KhongDau,'') as TenHangHoa_KhongDau,
    		concat(hh.TenHangHoa ,' ', dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    		hh.TenHangHoa,
    		dvqd.TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		lh.MaLoHang as TenLoHang,
    		hdct.SoLuong,
    		hdct.DonGia as GiaBan,
    		hdct.TienChietKhau,
    		nv.TenNhanVien,
			hdct.GhiChu,
    		hdct.ThanhTien,
			hdct.TienThue * hdct.SoLuong as TienThue,
			Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    		FROM BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    		inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_LoHang lh on hdct.ID_LoHang = lh.ID
    		left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    		left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    		left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    		where hd.NgayLapHoaDon between @timeStart and @timeEnd
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and hd.ChoThanhToan = 0 
    		and hd.LoaiHoaDon in (4,13,14)
    		and dvqd.Xoa like @TrangThai
    		and hh.TheoDoi like @TheoDoi
			and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where hh.ID_NhomHang= allnhh.ID))    			
    	) a
    	where (@ID_NhomNCC ='' or a.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomNCC)))		
		and	((select count(Name) from @tblSearchString b where 
    				a.TenHangHoa_KhongDau like '%'+b.Name+'%'     			
    				or a.TenHangHoaFull like '%'+b.Name+'%'
    				or a.TenLoHang like '%' +b.Name +'%' 
    				or a.MaHangHoa like '%'+b.Name+'%'
    				or a.MaHoaDon like '%'+b.Name+'%'
    				or a.GhiChu like '%'+b.Name+'%'    									
    				or a.TenDonViTinh like '%'+b.Name+'%'   			
					)=@count or @count=0)
    		and  (a.MaNhaCungCap like N'%'+@MaNCC +'%'
					or a.TenNhaCungCap like N'%'+ @MaNCC+'%'
					or a.TenDoiTuong_KhongDau like N'%'+ @MaNCC+'%'					
					or a.DienThoai like N'%'+ @MaNCC +'%')
    	order by a.NgayLapHoaDon desc
END");

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
							and hd.LoaiHoaDon = 1 ---- khong trahang HDSC
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
			nv.TenNhanVien,	
				--iif (cnLast.LoaiHoaDonGoc != 6, cnLast.TongTienHDTra,
				--	iif(cnLast.TongGiaTriTra > cnLast.ConNo1, cnLast.TongTienHDTra + cnLast.ConNo1,cnLast.TongTienHDTra)) as TongTienHDTra,
					
				iif (cnLast.LoaiHoaDonGoc != 6, cnLast.ConNo1,
					iif(cnLast.TongGiaTriTra > cnLast.ConNo1, 0, cnLast.ConNo1)) as ConNo
		from
		(
		select 
				tblLast.*,
				tblLast.TongThanhToan 
					--- neu hddoitra co LuyKeTraHang > 0 , thì gtrị bù trù = 0
					- iif(tblLast.LoaiHoaDonGoc = 6, iif(tblLast.LuyKeTraHang > 0, 0, abs(tblLast.LuyKeTraHang)), tblLast.LuyKeTraHang)
					- tblLast.KhachDaTra - isnull(thuBH.BaoHiemDaTra,0) as ConNo1 ---- ConNo = TongThanhToan - GtriBuTru
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

        }
        
        public override void Down()
        {
        }
    }
}
