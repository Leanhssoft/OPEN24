﻿namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class Update_20220405 : DbMigration
    {
        public override void Up()
        {
			CreateStoredProcedure(name: "[dbo].[BaoCaoHoatDongXe_ChiTiet]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				FromDate = p.String(),
				ToDate = p.String(),
				IDNhomHangs = p.String(),
				IDNhanViens = p.String(),
				IDNhomPhuTungs = p.String(),
				SoGioFrom = p.Int(),
				SoGioTo = p.Int(),
				TextSearch = p.String(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
	 
	declare @sql1 nvarchar(max),  @sql2 nvarchar(max), @sql nvarchar(max),
	@where1 nvarchar(max), @where2 nvarchar(max),@where3 nvarchar(max),
	@param nvarchar(max), @tbl nvarchar(max)

	set @where1 =' where 1 = 1 and hh.ID_Xe is not null '
	set @where2 =' where 1 = 1 and hh.ID_Xe is not null '
	set @where3 =' where 1 = 1 '

	if ISNULL(@CurrentPage,'')='' set @CurrentPage = 0
	if ISNULL(@PageSize,'')='' set @PageSize = 100

	if isnull(@IDChiNhanhs,'')!=''
		begin
			set @tbl= N' declare @tblChiNhanh table (ID uniqueidentifier)
						insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In) ' 
			set @where1 = CONCAT(@where1, N' and exists (select cn.ID from @tblChiNhanh cn where tn.ID_DonVi = cn.ID)')
		end
	if isnull(@IDNhanViens,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhanVien table (ID uniqueidentifier)
						insert into @tblNhanVien select name from dbo.splitstring(@IDNhanViens_In) ' )
			set @where1 = CONCAT(@where1, N' and exists (select nv.ID from @tblNhanVien nv where tn.ID_CoVanDichVu = nv.ID)')
		end
	if isnull(@IDNhomHangs,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhomHang table (ID uniqueidentifier)
						insert into @tblNhomHang select name from dbo.splitstring(@IDNhomHangs_In) ' )
			set @where2 = CONCAT(@where2, N' and exists (select nhom1.ID from @tblNhomHang nhom1 where nhomhh.ID = nhom1.ID)')
		end	

	if isnull(@IDNhomPhuTungs,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhomPhuTung table (ID uniqueidentifier)
						insert into @tblNhomPhuTung select name from dbo.splitstring(@IDNhomPhuTungs_In) ' )
			set @where2 = CONCAT(@where2, N' and exists (select nhom2.ID from @tblNhomPhuTung nhom2 where nhompt.ID = nhom2.ID)')
		end	


	if isnull(@TextSearch,'')!=''
		begin
			set @tbl= CONCAT(@tbl,N' DECLARE @tblSearch TABLE (Name [nvarchar](max))
					DECLARE @count int;
					INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!=''''
					Select @count =  (Select count(*) from @tblSearch) ' )
			set @where3 = CONCAT(@where3, N' AND ((select count(Name) from @tblSearch b where 
					tbl.BienSo like ''%''+b.Name+''%''
    				or tbl.TenHangHoa like ''%'' + b.Name +''%''
    				or tbl.MaHangHoa like ''%'' + b.Name +''%''    			
    				or tbl.TenHangHoa_KhongDau like ''%'' + b.Name +''%''	
					or tbl.TenPhuTung like ''%'' + b.Name +''%''
    				or tbl.MaPhuTung like ''%'' + b.Name +''%''    			
    				or tbl.TenPhuTung_KhongDau like ''%'' + b.Name +''%''	
					or tbl.MaPhieuTiepNhan like ''%'' + b.Name +''%''		
					)=@count or @count=0)')
		end
	
	if isnull(@FromDate,'')!=''
			set @where1 = CONCAT(@where1, N' and tn.NgayVaoXuong > @FromDate_In')	
	if isnull(@ToDate,'')!=''
			set @where1 = CONCAT(@where1, N' and tn.NgayVaoXuong < @ToDate_In')	
			
	if isnull(@SoGioFrom,'')!=''
			begin
				set @SoGioFrom = @SoGioFrom - 1
				set @where1= CONCAT(@where1, ' and tn.SoKmVao > @SoGioFrom_In')
			end
	if isnull(@SoGioTo,'')!=''
			begin
				set @SoGioTo = @SoGioTo + 1
				set @where1= CONCAT(@where1, ' and tn.SoKmVao < @SoGioTo_In')
			end
	
	set @sql1 = concat( N'		
			select tn.ID, tn.ID_DonVi, tn.ID_Xe, tn.ID_CoVanDichVu, tn.SoKmVao, tn.MaPhieuTiepNhan, tn.NgayVaoXuong, tn.GhiChu
			into #tblHoatDong
			from Gara_PhieuTiepNhan tn	
			left join DM_HangHoa hh on tn.ID_Xe= hh.ID_Xe ', @where1		
			)

	set @sql2 = concat( N'			
	---- get tpdinhluong by idhanghoa
	select hh.ID_Xe, 
		hh.ID as ID_HangHoa, 
		qd.MaHangHoa,
		hh.TenHangHoa,
		hh.TenHangHoa_KhongDau,
		tpdl.MaHangHoa as MaPhuTung, 
		hhdl.TenHangHoa as TenPhuTung,
		hhdl.TenHangHoa_KhongDau as TenPhuTung_KhongDau, 		
		tpdl.TenDonViTinh, 
		hhdl.LoaiBaoHanh, hhdl.ThoiGianBaoHanh,
		case hhdl.LoaiBaoHanh
			when 1 then hhdl.ThoiGianBaoHanh * 24   ---- ngay
			when 2 then hhdl.ThoiGianBaoHanh * 24 * 30   ---- thang
			when 3 then hhdl.ThoiGianBaoHanh * 24 * 365   ---- nam
			when 4 then hhdl.ThoiGianBaoHanh    ---- gio
		end as MocBaoHanh,
		nhomhh.TenNhomHangHoa,
		nhompt.TenNhomHangHoa as TenNhomPhuTung
	into #tblPhuTung
	from DM_HangHoa hh
	join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa
	left join DM_NhomHangHoa nhomhh on hh.ID_NhomHang = nhomhh.ID
	left join DinhLuongDichVu dl on qd.ID = dl.ID_DichVu
	left join DonViQuiDoi tpdl on dl.ID_DonViQuiDoi = tpdl.ID
	left join DM_HangHoa hhdl on tpdl.ID_HangHoa = hhdl.ID 
	left join DM_NhomHangHoa nhompt on hhdl.ID_NhomHang = nhompt.ID 
	', @where2)

	set @sql = CONCAT(@tbl,@sql1, @sql2, 
	N'
	select *,
		iif(tbl.BHConLai <0,N''Bảo hành'','''') as BHTrangThai,
		row_number() over (order by tbl.NgayVaoXuong desc) as RN
	into #tblView
	from
		(
		select dv.TenDonVi, xe.BienSo, hd.SoKmVao as SoGioHoatDong, 
			hd.MaPhieuTiepNhan, hd.NgayVaoXuong, hd.GhiChu,
			hd.ID_Xe, hd.ID_DonVi,
			hd.ID as ID_PhieuTiepNhan,
			pt.ID_HangHoa,
			pt.MaPhuTung, pt.TenPhuTung, pt.TenPhuTung_KhongDau,
			pt.MaHangHoa, pt.TenHangHoa, pt.TenHangHoa_KhongDau, pt.TenDonViTinh,
			pt.TenNhomHangHoa,
			pt.TenNhomPhuTung,
			pt.LoaiBaoHanh,
			pt.ThoiGianBaoHanh,
			pt.MocBaoHanh,
			pt.MocBaoHanh - hd.SoKmVao as BHConLai,
			nv.TenNhanVien
		from #tblHoatDong hd
		left join #tblPhuTung pt on hd.ID_Xe= pt.ID_Xe
		join DM_DonVi dv on hd.ID_DonVi = dv.ID
		join Gara_DanhMucXe xe on pt.ID_Xe= xe.ID 
		left join NS_NhanVien nv on hd.ID_CoVanDichVu= nv.ID 
	) tbl ', @where3 ,' order by tbl.NgayVaoXuong')

	set @sql = CONCAT(@sql, N' declare @TongSoGioHoatDong float, @TotalRow int 
								
								select @TongSoGioHoatDong = sum(a.SoGioHoatDong) , @TotalRow = max(TotalRow)
									from (
										select max(SoGioHoatDong) as SoGioHoatDong, max(RN) as TotalRow from #tblView group by ID_PhieuTiepNhan
										) a ',
								N' select * , 
									@TongSoGioHoatDong as TongSoGioHoatDong,
									@TotalRow as TotalRow
								from #tblView where Rn between (@CurrentPage_In * @PageSize_In) + 1 and @PageSize_In * (@CurrentPage_In + 1)')

	set @param = N'@IDChiNhanhs_In nvarchar(max) = null,
				@FromDate_In nvarchar(max) = null,
				@ToDate_In nvarchar(max) = null,
				@IDNhomHangs_In nvarchar(max) = null,
				@IDNhanViens_In nvarchar(max) = null,
				@IDNhomPhuTungs_In nvarchar(max) = null,
				@SoGioFrom_In int = null,
				@SoGioTo_In int = null,
				@TextSearch_In nvarchar(max) = null,
				@CurrentPage_In int = null,
				@PageSize_In int = null'

				print @sql
	
	exec sp_executesql @sql, @param,
					@IDChiNhanhs_In  = @IDChiNhanhs,
					@FromDate_In  = @FromDate,
					@ToDate_In  = @ToDate,
					@IDNhomHangs_In  = @IDNhomHangs,
					@IDNhanViens_In  = @IDNhanViens,
					@IDNhomPhuTungs_In  = @IDNhomPhuTungs,
					@SoGioFrom_In = @SoGioFrom,
					@SoGioTo_In = @SoGioTo,
					@TextSearch_In  = @TextSearch,
					@CurrentPage_In  = @CurrentPage,
					@PageSize_In  = @PageSize");

			CreateStoredProcedure(name: "[dbo].[BaoCaoHoatDongXe_TongHop]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(),
				ToDate = p.String(),
				IDNhomHangs = p.String(),
				IDNhanViens = p.String(),
				IDNhomPhuTungs = p.String(),
				TrangThai = p.Int(),
				TextSearch = p.String(),
				CurrentPage = p.Int(),
				PageSize = p.Int()
			}, body: @"SET NOCOUNT ON;
	 
	declare @sql1 nvarchar(max),  @sql2 nvarchar(max), @sql nvarchar(max),
	@where1 nvarchar(max), @where2 nvarchar(max),@where3 nvarchar(max),
	@param nvarchar(max), @tbl nvarchar(max)

	set @where1 =' where 1 = 1 and hh.ID_Xe is not null '
	set @where2 =' where 1 = 1 and hh.ID_Xe is not null '
	set @where3 =' where 1 = 1 '

	if ISNULL(@CurrentPage,'')='' set @CurrentPage = 0
	if ISNULL(@PageSize,'')='' set @PageSize = 100

	if isnull(@IDChiNhanhs,'')!=''
		begin
			set @tbl= N' declare @tblChiNhanh table (ID uniqueidentifier)
						insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs_In) ' 
			set @where1 = CONCAT(@where1, N' and exists (select cn.ID from @tblChiNhanh cn where tn.ID_DonVi = cn.ID)')
		end
	if isnull(@IDNhanViens,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhanVien table (ID uniqueidentifier)
						insert into @tblNhanVien select name from dbo.splitstring(@IDNhanViens_In) ' )
			set @where1 = CONCAT(@where1, N' and exists (select nv.ID from @tblNhanVien nv where tn.ID_NhanVien = nv.ID)')
		end
	if isnull(@IDNhomHangs,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhomHang table (ID uniqueidentifier)
						insert into @tblNhomHang select name from dbo.splitstring(@IDNhomHangs_In) ' )
			set @where2 = CONCAT(@where2, N' and exists (select nhom1.ID from @tblNhomHang nhom1 where nhomhh.ID = nhom1.ID)')
		end	

	if isnull(@IDNhomPhuTungs,'')!=''
		begin
			set @tbl= CONCAT(@tbl, N' declare @tblNhomPhuTung table (ID uniqueidentifier)
						insert into @tblNhomPhuTung select name from dbo.splitstring(@IDNhomPhuTungs_In) ' )
			set @where2 = CONCAT(@where2, N' and exists (select nhom2.ID from @tblNhomPhuTung nhom2 where nhompt.ID = nhom2.ID)')
		end	

	if isnull(@TextSearch,'')!=''
		begin
			set @tbl= CONCAT(@tbl,N' DECLARE @tblSearch TABLE (Name [nvarchar](max))
					DECLARE @count int;
					INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!=''''
					Select @count =  (Select count(*) from @tblSearch) ' )
			set @where3 = CONCAT(@where3, N' AND ((select count(Name) from @tblSearch b where 
					tbl.BienSo like ''%''+b.Name+''%''
    				or tbl.TenHangHoa like ''%'' + b.Name +''%''
    				or tbl.MaHangHoa like ''%'' + b.Name +''%''    			
    				or tbl.TenHangHoa_KhongDau like ''%'' + b.Name +''%''	
					or tbl.TenPhuTung like ''%'' + b.Name +''%''
    				or tbl.MaPhuTung like ''%'' + b.Name +''%''    			
    				or tbl.TenPhuTung_KhongDau like ''%'' + b.Name +''%''	
					)=@count or @count=0)')
		end
	if isnull(@ToDate,'')!=''
		begin
			set @ToDate= DATEADD(day,1,@ToDate)
			set @where1 = CONCAT(@where1, N' and tn.NgayVaoXuong < @ToDate_In')	
		end
			
	if isnull(@TrangThai,0)!=0 ---- 0.all, 1.đến hạn bảo hành, 2.ngược lại 1
			begin
				if isnull(@TrangThai,0) = 1
					set @where3 = CONCAT(@where3, N' and tbl.BHConLai < 1')	
				if isnull(@TrangThai,0) = 2
					set @where3 = CONCAT(@where3, N' and tbl.BHConLai > 0')	
			end
	
	set @sql1 = concat( N'
			---- sum sogiothuchien by car
			select tn.ID_DonVi, tn.ID_Xe, sum( isnull(tn.SoKmVao,0)) as SoGioHoatDong
			into #tblHoatDong
			from Gara_PhieuTiepNhan tn	
			left join DM_HangHoa hh on tn.ID_Xe= hh.ID_Xe ', @where1,		
			' group by tn.ID_Xe, tn.ID_DonVi')

	set @sql2 = concat( N'			
	---- get tpdinhluong by idhanghoa
	select hh.ID_Xe,
		hh.ID as ID_HangHoa, 
		qd.MaHangHoa,
		hh.TenHangHoa,
		hh.TenHangHoa_KhongDau,
		tpdl.MaHangHoa as MaPhuTung, 
		hhdl.TenHangHoa as TenPhuTung,
		hhdl.TenHangHoa_KhongDau as TenPhuTung_KhongDau, 		
		tpdl.TenDonViTinh, 
		hhdl.LoaiBaoHanh, hhdl.ThoiGianBaoHanh,
		case hhdl.LoaiBaoHanh
			when 1 then hhdl.ThoiGianBaoHanh * 24   ---- ngay
			when 2 then hhdl.ThoiGianBaoHanh * 24 * 30   ---- thang
			when 3 then hhdl.ThoiGianBaoHanh * 24 * 365   ---- nam
			when 4 then hhdl.ThoiGianBaoHanh    ---- gio
		end as MocBaoHanh,
		nhomhh.TenNhomHangHoa,
		nhompt.TenNhomHangHoa as TenNhomPhuTung
	into #tblPhuTung
	from DM_HangHoa hh
	join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa
	left join DM_NhomHangHoa nhomhh on hh.ID_NhomHang = nhomhh.ID
	left join DinhLuongDichVu dl on qd.ID = dl.ID_DichVu
	left join DonViQuiDoi tpdl on dl.ID_DonViQuiDoi = tpdl.ID
	left join DM_HangHoa hhdl on tpdl.ID_HangHoa = hhdl.ID 
	left join DM_NhomHangHoa nhompt on hhdl.ID_NhomHang = nhompt.ID 
	',@where2)

	set @sql = CONCAT(@tbl,@sql1, @sql2, 
	N'
	select *,
		iif(tbl.BHConLai <0,N''Bảo hành'','''') as BHTrangThai,
		row_number() over (order by tbl.SoGioHoatDong desc) as RN
	into #tblView
	from
		(
		select dv.TenDonVi, xe.BienSo, hd.SoGioHoatDong, 
			hd.ID_Xe, hd.ID_DonVi,
			pt.ID_HangHoa,
			pt.MaPhuTung, pt.TenPhuTung, pt.TenPhuTung_KhongDau,
			pt.MaHangHoa, pt.TenHangHoa, pt.TenHangHoa_KhongDau, pt.TenDonViTinh,
			pt.TenNhomHangHoa,
			pt.TenNhomPhuTung,
			pt.LoaiBaoHanh,
			pt.ThoiGianBaoHanh,
			isnull(pt.MocBaoHanh,0) as MocBaoHanh,
			pt.MocBaoHanh - hd.SoGioHoatDong as BHConLai
		from #tblHoatDong hd
		left join #tblPhuTung pt on hd.ID_Xe= pt.ID_Xe
		join DM_DonVi dv on hd.ID_DonVi = dv.ID
		join Gara_DanhMucXe xe on pt.ID_Xe= xe.ID 
	) tbl ', @where3)

	set @sql = CONCAT(@sql, N' declare @TongSoGioHoatDong float, @TotalRow int 
								
								select @TongSoGioHoatDong = sum(a.SoGioHoatDong) , @TotalRow = max(TotalRow)
									from (
										select max(SoGioHoatDong) as SoGioHoatDong, max(RN) as TotalRow from #tblView group by ID_Xe
										) a ',
								N' select * , 
									@TongSoGioHoatDong as TongSoGioHoatDong,
									@TotalRow as TotalRow
								from #tblView where Rn between (@CurrentPage_In * @PageSize_In) + 1 and @PageSize_In * (@CurrentPage_In + 1)')

	set @param = N'@IDChiNhanhs_In nvarchar(max) = null,
				@ToDate_In nvarchar(max) = null,
				@IDNhomHangs_In nvarchar(max) = null,
				@IDNhanViens_In nvarchar(max) = null,
				@IDNhomPhuTungs_In nvarchar(max) = null,
				@TrangThai_In int = null,
				@TextSearch_In nvarchar(max) = null,
				@CurrentPage_In int = null,
				@PageSize_In int = null'

				print @sql
	
	exec sp_executesql @sql, @param,
					@IDChiNhanhs_In  = @IDChiNhanhs,
					@ToDate_In  = @ToDate,
					@IDNhomHangs_In  = @IDNhomHangs,
					@IDNhanViens_In  = @IDNhanViens,
					@IDNhomPhuTungs_In  = @IDNhomPhuTungs,
					@TrangThai_In = @TrangThai,
					@TextSearch_In  = @TextSearch,
					@CurrentPage_In  = @CurrentPage,
					@PageSize_In  = @PageSize");

			CreateStoredProcedure(name: "[dbo].[GetInforHoaDon_ByID]", parametersAction: p => new
			{
				ID_HoaDon = p.String()
			}, body: @"SET NOCOUNT ON;

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
					and qct.ID_DoiTuong= @ID_DoiTuong					
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
					and qct.ID_DoiTuong= @ID_BaoHiem					
					group by qct.ID_HoaDonLienQuan
				)tblThuChi group by tblThuChi.ID
			) soquy on hd.ID = soquy.ID		
    	where hd.ID like @ID_HoaDon");

			CreateStoredProcedure(name: "[dbo].[HuyTienCoc_CheckVuotHanMuc]", parametersAction: p => new
			{
				ID_PhieuThuChi = p.Guid()
			}, body: @"SET NOCOUNT ON;
	declare @ngaylapPhieu datetime , @ID_DoiTuong uniqueidentifier, @ID_DonVi uniqueidentifier
	select top 1 
			@ngaylapPhieu = NgayLapHoaDon , 
			@ID_DoiTuong = ID_DoiTuong
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
	where qhd.ID= @ID_PhieuThuChi


	declare @tongnap float , @sudung float
	---- get tongtien napcoc den @ThoiGian
	---- ncc: tongnap (-)
	select @tongnap = ABS(sum(iif(qhd.LoaiHoaDon =11, qct.TienThu, -qct.TienThu))) 
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
	where qhd.TrangThai = '1'
	and qct.LoaiThanhToan =  1
	and qhd.NgayLapHoaDon < @ngaylapPhieu
	and qct.ID_DoiTuong= @ID_DoiTuong
	
	---- sudung tiencoc
	select @sudung = ABS(sum(iif(qhd.LoaiHoaDon =11, -qct.TienThu, qct.TienThu))) 
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
	where qhd.TrangThai = '1'
	and qct.HinhThucThanhToan= 6
	and qct.ID_DoiTuong= @ID_DoiTuong

	---- 0.chưa vượt hạn mức
	select CAST( iif( isnull(@tongnap,0) - isnull(@sudung,0) > - 1,'0','1') as bit) as Exist");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_HangTraLai]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang uniqueidentifier,
	@LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
set nocount on;
    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);
	SELECT 
		a.MaChungTuGoc, 
    	a.MaChungTu, 
    	a.NgayLapHoaDon,
    	a.MaHangHoa,
    	a.TenHangHoaFull,
    	a.TenHangHoa,
    	a.TenDonViTinh,
    	a.ThuocTinh_GiaTri,
    	a.TenLoHang,
    	CAST(ROUND(a.SoLuongTra, 3) as float) as SoLuong,
		CAST(ROUND(a.ThanhTien, 0) as float) as ThanhTien,
		CAST(ROUND(a.GiamGiaHD, 0) as float) as GiamGiaHD,
    	CAST(ROUND(a.ThanhTien - a.GiamGiaHD, 0) as float) as GiaTriTra,
    	a.TenNhanVien,
		a.GhiChu ,
		a.LoaiHoaDon
	FROM 
		(select 
			Case when hdb.ID is null then 1 else hdb.LoaiHoaDon end as LoaiHoaDon,
			Case when hdb.ID is null then N'HĐ trả nhanh' else hdb.MaHoaDon end as MaChungTuGoc,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			hdt.MaHoaDon as MaChungTu,
			hdt.NgayLapHoaDon,
			dvqd.MaHangHoa,
			concat(hh.TenHangHoa , dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
			hh.TenHangHoa,
			dvqd.TenDonViTinh as TenDonViTinh,
			dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			lh.MaLoHang as TenLoHang,
			hdct.SoLuong as SoLuongTra,
			Case when hdt.TongTienHang = 0 then 0 else hdct.ThanhTien * ((hdt.TongGiamGia + hdt.KhuyeMai_GiamGia) / hdt.TongTienHang) end as GiamGiaHD,
			hdct.ThanhTien as ThanhTien,
			nv.TenNhanVien,
			hdct.GhiChu 
			from BH_HoaDon hdt
			LEFT JOIN BH_HoaDon hdb ON hdt.ID_HoaDon = hdb.ID
			INNER JOIN BH_HoaDon_ChiTiet hdct ON hdct.ID_HoaDon = hdt.ID			
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
			inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
			left join NS_NhanVien nv on hdt.ID_NhanVien = nv.ID
			left join DM_LoHang lh on hdct.ID_LoHang = lh.ID AND hh.ID = lh.ID_HangHoa
			INNER JOIN (SELECT ID FROM dbo.GetListNhomHangHoa(null)) allnhh
			ON hh.ID_NhomHang = allnhh.ID
			INNER JOIN (select * from splitstring(@ID_ChiNhanh)) lstID_DonVi
			ON lstID_DonVi.Name = hdt.ID_DonVi
			WHERE hdt.LoaiHoaDon = 6
			and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null) 
			and (hdct.ID_ParentCombo = hdct.ID or hdct.ID_ParentCombo is null)
			AND hdt.NgayLapHoaDon >= @timeStart AND hdt.NgayLapHoaDon < @timeEnd
			AND hdt.ChoThanhToan = 0
    		and hh.TheoDoi like @TheoDoi
    		and dvqd.Xoa like @TrangThai
			AND ((select count(Name) from @tblSearchString b where 
			hdt.MaHoaDon like '%'+b.Name+'%' 
    		OR hdb.MaHoaDon like '%'+b.Name+'%' 
    		or dvqd.MaHangHoa like '%'+b.Name+'%' 
    			or hh.TenHangHoa like '%'+b.Name+'%'
    			or hh.TenHangHoa_KhongDau like '%' +b.Name +'%' 
				or hh.TenHangHoa_KyTuDau like '%' +b.Name +'%'
    		or lh.MaLoHang like '%'+b.Name+'%'
			or nv.MaNhanVien like '%'+b.Name+'%'
    			or nv.TenNhanVien like '%'+b.Name+'%'
    			or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
				or dvqd.TenDonViTinh like '%'+b.Name+'%'
				or dvqd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)) a
	WHERE a.LoaiHoaDon in (select * from splitstring(@LoaiChungTu))
	and a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))
	ORDER BY a.NgayLapHoaDon DESC
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapChuyenHangChiTiet]
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER,
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	  SET NOCOUNT ON;
	 DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	 INSERT INTO @tblIdDonVi
	 SELECT Name FROM [dbo].[splitstring](@ID_ChiNhanh) 

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From HT_NguoiDung nd	    		
    		where nd.ID = @ID_NguoiDung);


				select 
					CNChuyen.TenDonVi as ChiNhanhChuyen,
					CNnhan.TenDonVi as ChiNhanhNhan,
					tblHD.NgayLapHoaDon,tblHD.MaHoaDon,
					isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
					isnull(lo.MaLoHang,'') as TenLoHang,
					qd.MaHangHoa, qd.TenDonViTinh, 
					isnull(qd.ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
					hh.TenHangHoa,
					CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,				
					round(tblHD.SoLuong, 3) as SoLuong,
					iif(@XemGiaVon='1',round(tblHD.DonGia,3),0) as DonGia,
					iif(@XemGiaVon='1',round(tblHD.GiaVon,3),0) as GiaVon,
					iif(@XemGiaVon='1',round(tblHD.ThanhTien,3),0) as ThanhTien,
					iif(@XemGiaVon='1',round(tblHD.GiaTri,3),0) as GiaTri			
				from(
					select 
						qd.ID_HangHoa,tblHD.ID_LoHang, 
						tblHD.ID_DonVi,tblHD.ID_CheckIn, tblHD.NgayLapHoaDon,tblHD.MaHoaDon,
						sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
						max(tblHD.GiaVon / iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as GiaVon,
						max(tblHD.DonGia / iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as DonGia,
						sum(tblHD.ThanhTien) as ThanhTien,
						sum(tblHD.GiaTri) as GiaTri
					from(
					select ct.ID_DonViQuiDoi, ct.ID_LoHang, 
						hd.ID_DonVi, hd.ID_CheckIn, hd.NgayLapHoaDon, hd.MaHoaDon,
						sum(ct.TienChietKhau) as SoLuong,
						max(ct.GiaVon) as GiaVon,
						max(ct.DonGia) as DonGia,
						sum(ct.TienChietKhau * ct.DonGia) as ThanhTien, --- get gtri nhan
						sum(ct.TienChietKhau * ct.GiaVon) as GiaTri
					from BH_HoaDon_ChiTiet ct
					join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
					where hd.ChoThanhToan=0
					and hd.LoaiHoaDon= 10 and (hd.YeuCau='1' or hd.YeuCau='4') --- YeuCau: 1.DangChuyen, 4.DaNhan, 2.PhieuTam, 3.Huy
					and hd.NgaySua >=@timeStart and hd.NgaySua < @timeEnd
					and exists (select ID from @tblIdDonVi dv where hd.ID_CheckIn= dv.ID)
					group by ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi, hd.ID_CheckIn,hd.NgayLapHoaDon, hd.MaHoaDon
					)tblHD
					join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID
					group by qd.ID_HangHoa, tblHD.ID_DonViQuiDoi,tblHD.ID_LoHang, tblHD.ID_DonVi,tblHD.ID_CheckIn,tblHD.NgayLapHoaDon, tblHD.MaHoaDon
				)tblHD
				join DM_DonVi CNChuyen on tblHD.ID_DonVi = CNChuyen.ID
				left join DM_DonVi CNnhan on tblHD.ID_CheckIn= CNnhan.ID
				join DM_HangHoa hh on tblHD.ID_HangHoa= hh.ID
				join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
				left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
				left join DM_LoHang lo on tblHD.ID_LoHang= lo.ID and (lo.ID= tblHD.ID_LoHang or (tblHD.ID_LoHang is null and lo.ID is null))
				where hh.LaHangHoa = 1
				and hh.TheoDoi like @TheoDoi
				and qd.Xoa like @TrangThai
				and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID= allnhh.ID)
				AND ((select count(Name) from @tblSearchString b where 
    			hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa like '%'+b.Name+'%'
    			or lo.MaLoHang like '%' +b.Name +'%' 
				or qd.MaHangHoa like '%'+b.Name+'%'
				or qd.MaHangHoa like '%'+b.Name+'%'
    			or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    			or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    			or qd.TenDonViTinh like '%'+b.Name+'%'
    			or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
				or CNnhan.TenDonVi like '%'+b.Name+'%'
				or CNChuyen.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
				order by tblHD.NgayLapHoaDon desc, hh.TenHangHoa, lo.MaLoHang	 
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_XuatDichVuDinhLuong]
    @SearchString [nvarchar](max),
    @LoaiChungTu [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER);
	 INSERT INTO @tblIdDonVi
	 SELECT Name FROM [dbo].[splitstring](@ID_ChiNhanh) 

	 DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From HT_NguoiDung nd	   		
    		where nd.ID = @ID_NguoiDung)

			select 
				ctsdQD.MaHoaDon,
				ctsdQD.NgayLapHoaDon,
				ctsdQD.TenLoaiChungTu as LoaiHoaDon,
				dv.TenDonVi,
				dv.MaDonVi,
				isnull(nv.TenNhanVien,'') as TenNhanVien,
				isnull(tn.MaPhieuTiepNhan,'') as MaPhieuTiepNhan,
				isnull(xe.BienSo,'') as BienSo,

				-- dichvu
				qddv.MaHangHoa as MaDichVu,
				qddv.TenDonViTinh as TenDonViDichVu,
				qddv.ThuocTinhGiaTri as ThuocTinhDichVu,
				hhdv.TenHangHoa as TenDichVu,
				ctsdQD.ID_DichVu,
				ctsdQD.SoLuongDichVu,
				ctsdQD.GiaTriDichVu,
				ISNULL(nhomDV.TenNhomHangHoa, N'Nhóm Dịch Vụ Mặc Định') as NhomDichVu,

				-- dinhluong
				ctsdQD.ID_DonViQuiDoi,						
				ctsdQD.SoLuongXuat as SoLuongThucTe,
				iif(@XemGiaVon='1', ctsdQD.GiaTriXuat,0) as GiaTriThucTe,
				ctsdQD.SoLuongDinhLuongBanDau,				
				iif(@XemGiaVon='1', ctsdQD.GiaTriDinhLuongBanDau,0) as GiaTriDinhLuongBanDau,
				qddl.MaHangHoa,
				qddl.TenDonViTinh,
				qddl.ThuocTinhGiaTri,
				ctsdQD.GhiChu,
				hhdl.TenHangHoa,
				concat(hhdl.TenHangHoa, qddl.ThuocTinhGiaTri) as TenHangHoaFull,
				ISNULL(nhomHH.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,


				ctsdQD.SoLuongXuat - ctsdQD.SoLuongDinhLuongBanDau as SoLuongChenhLech,
				iif(@XemGiaVon='1', ctsdQD.GiaTriXuat - ctsdQD.GiaTriDinhLuongBanDau,0) as GiaTriChenhLech,

				Case when ctsdQD.SoLuongXuat = 0 then N'Không xuất'
				when ctsdQD.SoLuongChenhLech < 0 then N'Xuất thiếu'
				when ctsdQD.SoLuongChenhLech = 0 then N'Xuất đủ'
				when (ctsdQD.SoLuongDinhLuongBanDau = 0) and ctsdQD.SoLuongXuat > 0 then N'Xuất thêm'
				else N'Xuất thừa' end as TrangThai

			from
			(select 
				ctsd.MaHoaDon,
				ctsd.NgayLapHoaDon,
				ctsd.ID_DonVi,
				ctsd.ID_NhanVien,
				ctsd.ID_PhieuTiepNhan,

				ctsd.LoaiHoaDon,
				case ctsd.LoaiHoaDon
					when 2 then N'Xuất sử dụng gói dịch vụ'
					when 3 then N'Xuất bán dịch vụ định lượng'
					when 11 then N'Xuất sửa chữa'
					else '' end TenLoaiChungTu,
				ctsd.SoLuongXuat * iif(qddl.TyLeChuyenDoi=0,1, qddl.TyLeChuyenDoi) as SoLuongXuat,
				ctsd.SoLuongDinhLuongBanDau * iif(qddl.TyLeChuyenDoi=0,1, qddl.TyLeChuyenDoi) as SoLuongDinhLuongBanDau,

				ctsd.SoLuongXuat- ctsd.SoLuongDinhLuongBanDau as SoLuongChenhLech,

				ctsd.ID_DonViQuiDoi,
				ctsd.ID_DichVu,
				ctsd.SoLuongDichVu,
				ctsd.GiaTriDichVu,
				ctsd.GiaTriXuat,
				ctsd.GiaTriDinhLuongBanDau,
				ctsd.GhiChu,
				qddl.ID_HangHoa

			from(

			select 
				a.MaHoaDon,
				a.ID_DonVi,
				a.ID_NhanVien,
				a.NgayLapHoaDon,
				a.ID_PhieuTiepNhan,
				a.LoaiHoaDon,
				a.ID_DichVu, 
				a.SoLuongDichVu,
				a.GiaTriDichVu,
				max(SoLuongDinhLuongBanDau) as SoLuongDinhLuongBanDau,			
				max(GiaTriDinhLuongBanDau) as GiaTriDinhLuongBanDau,			
				sum(isnull(SoLuongXuat, 0)) as SoLuongXuat,
				sum(isnull(GiaTriXuat, 0)) as GiaTriXuat,
				(a.ID_DonViQuiDoi) as ID_DonViQuiDoi,
				max(isnull(a.GhiChu,'')) as GhiChu
			from
			(
					--- xuatban, xuatsudung
					select 
						hd.MaHoaDon,
						hd.ID_DonVi,
						hd.ID_NhanVien,
						hd.NgayLapHoaDon,
						hd.ID_PhieuTiepNhan,
    					ctdl.ID_DonViQuiDoi,
						ctdl.ID_LoHang,
						ctdl.GhiChu,					 
    						case when hd.LoaiHoaDon= 25 then 11 -- xuatkho sc
						else
    					Case when ctdl.ID_ChiTietGoiDV is not null
							then case when hd.ID_HoaDon is null then 2 else 3 end
						when ctdl.ID_ChiTietGoiDV is null and ctdl.ID_ChiTietDinhLuong is not null then 3 
						else case when hd.LoaiHoaDon = 8 then 11 else hd.LoaiHoaDon end end end as LoaiHoaDon, 	
    			
						ctmua.ID_DonViQuiDoi as ID_DichVu,		
						ISNULL(ctmua.SoLuong,0) AS SoLuongDichVu,
						ISNULL(ctmua.SoLuong,0)* ctmua.GiaVon AS GiaTriDichVu,

    					iif(hd.LoaiHoaDon=25,isnull(xkhdsc.SoLuongXuat,0), ISNULL(ctdl.SoLuong,0)) AS SoLuongXuat,
						iif(hd.LoaiHoaDon= 25,isnull(xkhdsc.GiaTriXuat,0), ISNULL(ctdl.SoLuong,0) * ctdl.GiaVon) AS GiaTriXuat,

						iif(hd.LoaiHoaDon=25,isnull(ctdl.SoLuong,0),ISNULL(ctdl.SoLuongDinhLuong_BanDau,0)) AS SoLuongDinhLuongBanDau,
						iif(hd.LoaiHoaDon=25,isnull(ctdl.SoLuong,0),ISNULL(ctdl.SoLuongDinhLuong_BanDau,0)) * ctdl.GiaVon AS GiaTriDinhLuongBanDau,
						0 as LaDinhLuongBoSung

					from BH_HoaDon_ChiTiet ctdl
					join BH_HoaDon hd on ctdl.ID_HoaDon = hd.ID 
					left join BH_HoaDon_ChiTiet ctmua on ctmua.ID = ctdl.ID_ChiTietDinhLuong
					left join
					(
						select 
						hd.ID_HoaDon,
						ctxk.ID_ChiTietGoiDV,
						ctxk.ID_DonViQuiDoi,
						max(isnull(ctxk.GhiChu,'')) as GhiChu,					
						sum(ISNULL(ctxk.SoLuong,0)) AS SoLuongXuat,
						sum(ISNULL(ctxk.SoLuong,0)* ctxk.GiaVon) AS GiaTriXuat				
						from BH_HoaDon_ChiTiet ctxk
						join BH_HoaDon hd on ctxk.ID_HoaDon = hd.ID 
						where hd.ChoThanhToan='0' 
						and hd.LoaiHoaDon = 8
						group by hd.ID_HoaDon, ctxk.ID_ChiTietGoiDV,  ctxk.ID_DonViQuiDoi
					) xkhdsc on hd.ID= xkhdsc.ID_HoaDon and xkhdsc.ID_ChiTietGoiDV= ctdl.ID --and xkhdsc.ID_DonViQuiDoi = ctdl.ID_DonViQuiDoi
					where hd.ChoThanhToan='0' 
					and hd.LoaiHoaDon in ( 1,25)
					and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
					AND ctdl.ID_ChiTietDinhLuong is not null -- thành phần định lượng
					AND ctdl.ID_ChiTietDinhLuong != ctdl.ID		

					---- get dinhluong them vao khi tao phieu xuatkho sua chua (ID_ChiTietGoiDV la dichvu)
							union all					

							select 
								hdm.MaHoaDon,
								hdm.ID_DonVi,
								hdm.ID_NhanVien,
								hdm.NgayLapHoaDon,
								hdm.ID_PhieuTiepNhan,
								ctxkThem.ID_DonViQuiDoi,
								ctxkThem.ID_LoHang,
								ctxkThem.GhiChu,					 
    							11 as LoaiHoaDon,
    			
								ctm.ID_DonViQuiDoi as ID_DichVu,		
								ISNULL(ctm.SoLuong,0) AS SoLuongDichVu,
								ISNULL(ctm.SoLuong,0)* ctm.GiaVon AS GiaTriDichVu,

    							isnull(ctxkThem.SoLuong,0) AS SoLuongXuat,
								isnull(ctxkThem.SoLuong * ctxkThem.GiaVon,0) AS GiaTriXuat,

								0 AS SoLuongDinhLuongBanDau,
								0 AS GiaTriDinhLuongBanDau,
								1 as LaDinhLuongBoSung
				
							from BH_HoaDon_ChiTiet ctm
							join BH_HoaDon hdm on ctm.ID_HoaDon= hdm.ID
							left join BH_HoaDon_ChiTiet ctxkThem on ctm.ID = ctxkThem.ID_ChiTietGoiDV
							left join BH_HoaDon hdxk on ctxkThem.ID_HoaDon= hdxk.ID and hdm.ID= hdxk.ID_HoaDon
							where hdm.LoaiHoaDon= 25 and hdxk.LoaiHoaDon= 8
							and hdxk.ChoThanhToan='0'
							and hdm.ChoThanhToan='0'
							and ctm.ID = ctm.ID_ChiTietDinhLuong --- chi get dichvu			
							and hdm.NgayLapHoaDon >= @timeStart and hdm.NgayLapHoaDon < @timeEnd
							and exists (select id from @tblIdDonVi dv2 where hdm.ID_DonVi= dv2.ID)
				) a group by
						a.MaHoaDon,
						a.ID_DonVi,
						a.ID_NhanVien,
						a.NgayLapHoaDon,
						a.ID_PhieuTiepNhan,
						a.LoaiHoaDon,
						a.ID_DichVu, a.SoLuongDichVu, a.GiaTriDichVu ,
						a.ID_DonViQuiDoi, a.ID_LoHang
		) ctsd
		join DonViQuiDoi qddl on ctsd.ID_DonViQuiDoi = qddl.ID
		) ctsdQD
		left join DonViQuiDoi qddv on ctsdQD.ID_DichVu = qddv.ID
		left join DonViQuiDoi qddl on ctsdQD.ID_HangHoa = qddl.ID_HangHoa and qddl.LaDonViChuan= 1
		left join DM_DonVi dv on ctsdQD.ID_DonVi = dv.ID
		left join DM_HangHoa hhdl on qddl.ID_HangHoa = hhdl.ID
		left join DM_HangHoa hhdv on qddv.ID_HangHoa = hhdv.ID 
		left join DM_NhomHangHoa nhomHH on hhdl.ID_NhomHang= nhomHH.ID
		left join DM_NhomHangHoa nhomDV on hhdv.ID_NhomHang= nhomDV.ID
		left join NS_NhanVien nv on ctsdQD.ID_NhanVien= nv.ID	
		left join Gara_PhieuTiepNhan tn on ctsdQD.ID_PhieuTiepNhan= tn.ID
		left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
    	where exists (select Name from splitstring(@LoaiChungTu) ct where ctsdQD.LoaiHoaDon = ct.Name ) 				
    		and
			hhdv.TheoDoi like @TheoDoi			
			and qddv.Xoa like @TrangThai
			and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhomDV.ID = allnhh.ID )
			and exists (select id from @tblIdDonVi dv2 where dv.ID= dv2.ID)
			and
			 ((select count(Name) from @tblSearchString b where 
    			ctsdQD.MaHoaDon like '%'+b.Name+'%' 
				or ctsdQD.GhiChu like '%'+b.Name+'%'
    			or qddl.MaHangHoa like '%'+b.Name+'%' 
    			or hhdv.TenHangHoa like '%'+b.Name+'%'
    			or hhdv.TenHangHoa_KhongDau like '%' +b.Name +'%' 
				or hhdl.TenHangHoa like '%'+b.Name+'%'
    			or hhdl.TenHangHoa_KhongDau like '%'+b.Name+'%'
    			or nhomDV.TenNhomHangHoa like '%'+b.Name+'%'
    			or nhomHH.TenNhomHangHoa like '%'+b.Name+'%'
    			or qddv.MaHangHoa like '%'+b.Name+'%' 
    			or TenNhanVien like '%'+b.Name+'%'    
				or tn.MaPhieuTiepNhan like '%'+b.Name+'%'    
				or xe.BienSo like '%'+b.Name+'%' 
				)=@count or @count=0)
		ORDER BY NgayLapHoaDon DESC, qddv.MaHangHoa
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoNhapHang_TraHangNhap]
    @TextSearch [nvarchar](max),
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
    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)

			DECLARE @tblSearchString TABLE (Name [nvarchar](max));
			DECLARE @count int;
			INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
			Select @count =  (Select count(*) from @tblSearchString);
    	
    SELECT 
    	a.MaChungTuGoc, 
    	a.MaChungTu, 
    	a.NgayLapHoaDon,
    	a.MaHangHoa,
    	a.TenHangHoaFull,
    	a.TenHangHoa,
    	a.TenDonViTinh,
    	a.ThuocTinh_GiaTri,
    	a.TenLoHang,
    	CAST(ROUND(a.SoLuongTra, 3) as float) as SoLuong,
    	Case When @XemGiaVon = '1' then CAST(ROUND(a.ThanhTien, 0) as float) else 0 end as ThanhTien,
    	Case When @XemGiaVon = '1' then CAST(ROUND(a.GiamGiaHD, 0) as float) else 0 end as GiamGiaHD,
    	Case When @XemGiaVon = '1' then CAST(ROUND(a.ThanhTien -a.GiamGiaHD, 0) as float) else 0 end as GiaTriTra,
    		a.TenNhanVien
    	FROM
    	(
    		SELECT
    		Case when hdb.ID is null then N'HD trả nhanh' else hdb.MaHoaDon end as MaChungTuGoc,
    		hdt.MaHoaDon as MaChungTu,
    		hdt.NgayLapHoaDon,
    		dvqd.MaHangHoa,
    		hh.TenHangHoa + dvqd.ThuocTinhGiaTri as TenHangHoaFull,
    		hh.TenHangHoa,
    			dvqd.TenDonViTinh as TenDonViTinh,
    		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		lh.MaLoHang  as TenLoHang,
    		hdct.SoLuong as SoLuongTra,
			Case when hdt.TongTienHang = 0 then 0 else hdct.ThanhTien * (hdt.TongGiamGia / hdt.TongTienHang) end as GiamGiaHD,
    		hdct.ThanhTien,
    		nv.TenNhanVien
    		FROM
    		BH_HoaDon hdt 
    		left join BH_HoaDon hdb on hdt.ID_HoaDon = hdb.ID
    		join BH_HoaDon_ChiTiet hdct on hdt.ID = hdct.ID_HoaDon 
    		left join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		left join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join NS_NhanVien nv on hdt.ID_NhanVien = nv.ID
    		left join DM_LoHang lh on hdct.ID_LoHang = lh.ID    			
    		where hdt.NgayLapHoaDon >= @timeStart and hdt.NgayLapHoaDon < @timeEnd
    		and hdt.ChoThanhToan = 0
    		and hdt.ID_DonVi in (Select * from splitstring(@ID_ChiNhanh))
    		and hdt.LoaiHoaDon = 7    		
    		and hh.TheoDoi like @TheoDoi
			and dvqd.Xoa like @TrangThai
			and (@ID_NhomHang is null or exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where hh.ID_NhomHang= allnhh.ID))
			AND
						((select count(Name) from @tblSearchString b where 
    							dvqd.MaHangHoa like '%'+b.Name+'%' 
    							or hh.TenHangHoa like '%'+b.Name+'%' 
    							or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'
    							or hdt.MaHoaDon like '%' +b.Name +'%' 
    							)=@count or @count=0)	
    		
    	) a    	
    	ORDER BY a.NgayLapHoaDon DESC
END");

            Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTaiChinh_SoQuy_v2]
    @TextSearch [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @loaiKH [nvarchar](max),
    @ID_NhomDoiTuong [nvarchar](max),
    @lstThuChi [nvarchar](max),
    @HachToanKD [bit],
    @LoaiTien [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
    --	tinh ton dau ky
    	Declare @TonDauKy float
    	Set @TonDauKy = (Select
    	CAST(ROUND(SUM(TienThu - TienChi), 0) as float) as TonDauKy
    	FROM
    	(
    		select 
    			case when qhd.LoaiHoaDon = 11 then qhdct.TienThu else 0 end as TienThu,
    			Case when qhd.LoaiHoaDon = 12 then qhdct.TienThu else 0 end as TienChi,
    			Case when qhdct.TienMat > 0 and qhdct.TienGui = 0 then '1' 
    			when qhdct.TienGui > 0 and qhdct.TienMat = 0 then '2'
    			when qhdct.TienGui > 0 and qhdct.TienMat > 0 then '12' else '' end as LoaiTien,
    				qhd.HachToanKinhDoanh as HachToanKinhDoanh
    		From Quy_HoaDon qhd 
    		inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    		where qhd.NgayLapHoaDon < @timeStart
    		and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    			and (qhd.PhieuDieuChinhCongNo !='1' or qhd.PhieuDieuChinhCongNo is null)
    		and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and (qhdct.DiemThanhToan is null OR qhdct.DiemThanhToan = 0)
    			and qhdct.HinhThucThanhToan not in (4,5,6)
    		) a 
    		where LoaiTien like @LoaiTien
    			and (HachToanKinhDoanh = @HachToanKD OR @HachToanKD IS NULL)
    	) 
    		
    	if (@TonDauKy is null)
    	BeGin
    		Set @TonDauKy = 0;
    	END
    	Declare @tmp table (ID_HoaDon UNIQUEIDENTIFIER,MaPhieuThu nvarchar(max), NgayLapHoaDon datetime, KhoanMuc nvarchar(max), TenDoiTac nvarchar(max),
    	TienMat float, TienGui float, TienThu float, TienChi float, ThuTienMat float, ChiTienMat float, ThuTienGui float, ChiTienGui float, TonLuyKeTienMat float,TonLuyKeTienGui float,TonLuyKe float, SoTaiKhoan nvarchar(max), NganHang nvarchar(max), GhiChu nvarchar(max),
    		IDDonVi UNIQUEIDENTIFIER, TenDonVi NVARCHAR(MAX));
    	Insert INTO @tmp
    		 SELECT 
    				b.ID_HoaDon,
    				b.MaPhieuThu as MaPhieuThu,
    			b.NgayLapHoaDon as NgayLapHoaDon,
    				MAX(b.NoiDungThuChi) as KhoanMuc,
    			MAX(b.TenNguoiNop) as TenDoiTac, 
    			SUM (b.TienMat) as TienMat,
    			SUM (b.TienGui) as TienGui,
    			SUM (b.TienThu) as TienThu,
    			SUM (b.TienChi) as TienChi,
    			SUM (b.ThuTienMat) as ThuTienMat,
    			SUM (b.ChiTienMat) as ChiTienMat, 
    			SUM (b.ThuTienGui) as ThuTienGui,
    			SUM (b.ChiTienGui) as ChiTienGui, 
    				0 as TonLuyKe,
    			0 as TonLuyKeTienMat,
    			0 as TonLuyKeTienGui,
    			MAX(b.SoTaiKhoan) as SoTaiKhoan,
    			MAX(b.NganHang) as NganHang,
    			MAX(b.GhiChu) as GhiChu,
    				dv.ID,
    				dv.TenDonVi
    		FROM
    		(
    				select 
    			a.HachToanKinhDoanh,
    			a.ID_DoiTuong,
    			a.ID_HoaDon,
    			a.MaHoaDon,
    			a.MaPhieuThu,
    			a.NgayLapHoaDon,
    			a.TenNguoiNop,
    			a.TienMat,
    			a.TienGui,
    			case when a.LoaiHoaDon = 11 then a.TienGui else 0 end as ThuTienGui,
    			Case when a.LoaiHoaDon = 12 then a.TienGui else 0 end as ChiTienGui,
    			case when a.LoaiHoaDon = 11 then a.TienMat else 0 end as ThuTienMat,
    			Case when a.LoaiHoaDon = 12 then a.TienMat else 0 end as ChiTienMat,
    			case when a.LoaiHoaDon = 11 then a.TienThu else 0 end as TienThu,
    			Case when a.LoaiHoaDon = 12 then a.TienThu else 0 end as TienChi,
    			a.NoiDungThuChi,
    			a.NganHang,
    			a.SoTaiKhoan,
    			a.GhiChu,
    			Case when a.TienMat > 0 and TienGui = 0 then '1'  
    			 when a.TienGui > 0 and TienMat = 0 then '2' 
    			 when a.TienGui > 0 and TienMat > 0 then '12' else '' end  as LoaiTien,
    				a.ID_DonVi
    		From
    		(
    		select 
    			qhd.LoaiHoaDon,
    			MAX(qhd.ID) as ID_HoaDon,
    			MAX(dt.ID) as ID_DoiTuong,
    			MAX(ktc.NoiDungThuChi) as NoiDungThuChi,
    			MAX (tknh.SoTaiKhoan) as SoTaiKhoan,
    			MAX (nh.TenNganHang) as NganHang,
    			qhd.HachToanKinhDoanh,
    			Case when qhd.LoaiHoaDon = 11 and hd.LoaiHoaDon is null then 1 -- phiếu thu khác
    			when (qhd.LoaiHoaDon = 12 and hd.LoaiHoaDon is null) or ((hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19) and qhd.LoaiHoaDon = 12) then 2-- phiếu chi khác
    			when (hd.LoaiHoaDon = 1 or hd.LoaiHoaDon = 3 or hd.LoaiHoaDon = 19 or hd.LoaiHoaDon = 22 or hd.LoaiHoaDon = 25) and qhd.LoaiHoaDon = 11 then 3 -- bán hàng 
    			when hd.LoaiHoaDon = 6  then 4 -- Đổi trả hàng
    			when hd.LoaiHoaDon = 7 then 5 -- trả hàng NCC
    			when hd.LoaiHoaDon = 4 then 6 else 4 end as LoaiThuChi, -- nhập hàng NCC
    			dt.MaDoiTuong as MaKhachHang,
    			dt.DienThoai,
    			qhd.MaHoaDon as MaPhieuThu,
    			qhd.NguoiNopTien as TenNguoiNop,
    			max(IIF(qhdct.HinhThucThanhToan = 1, qhdct.TienThu, 0)) as TienMat,
    			max(IIF(qhdct.HinhThucThanhToan IN (2,3) , qhdct.TienThu, 0)) as TienGui,
    			max(qhdct.TienThu) as TienThu,
    			qhd.NgayLapHoaDon,
    			MAX(qhd.NoiDungThu) as GhiChu,
    			hd.MaHoaDon,
    				qhd.ID_DonVi
    		From Quy_HoaDon qhd 
    			inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    			left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    			left join DM_DoiTuong dt on qhdct.ID_DoiTuong = dt.ID
    			left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    			left join Quy_KhoanThuChi ktc on qhdct.ID_KhoanThuChi = ktc.ID
    			left join DM_TaiKhoanNganHang tknh on qhdct.ID_TaiKhoanNganHang = tknh.ID
    			left join DM_NganHang nh on qhdct.ID_NganHang = nh.ID
    		where qhd.NgayLapHoaDon BETWEEN @timeStart AND @timeEnd
    			and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    				and (qhd.PhieuDieuChinhCongNo !='1' or qhd.PhieuDieuChinhCongNo is null)
    			and (IIF(qhdct.ID_NhanVien is not null, 4, IIF(dt.loaidoituong IS NULL, 1, dt.LoaiDoiTuong)) in (select * from splitstring(@loaiKH)))
    			and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    			and (qhdct.DiemThanhToan is null OR qhdct.DiemThanhToan = 0)
    			and (qhd.HachToanKinhDoanh = @HachToanKD OR @HachToanKD IS NULL)
    				and (dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) OR @ID_NhomDoiTuong = '')
    				and qhdct.HinhThucThanhToan not in (4,5,6)
    				AND ((select count(Name) from @tblSearch b where     			
    			dt.TenDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    				or qhd.MaHoaDon like '%' + b.Name + '%'
    				or qhd.NguoiNopTien like '%' + b.Name + '%'
    			)=@count or @count=0)
    		Group by qhd.LoaiHoaDon, hd.LoaiHoaDon, dt.MaDoiTuong,dt.LoaiDoiTuong, dt.TenDoiTuong_ChuCaiDau, dt.TenDoiTuong_KhongDau,qhdct.ID_NhanVien,
    			 qhd.HachToanKinhDoanh, dt.DienThoai, qhd.MaHoaDon, qhd.NguoiNopTien, qhd.NgayLapHoaDon, hd.MaHoaDon, qhd.ID_DonVi, qhdct.ID, qhdct.HinhThucThanhToan
    		)a
    		where a.LoaiThuChi in (select * from splitstring(@lstThuChi))
    		) b
    			inner join DM_DonVi dv ON dv.ID = b.ID_DonVi
    			where LoaiTien like @LoaiTien
    		Group by b.ID_HoaDon, b.ID_DoiTuong, b.MaPhieuThu, b.NgayLapHoaDon, dv.TenDonVi, dv.ID
    		ORDER BY NgayLapHoaDon
    -- tính tồn lũy kế
    	    IF (EXISTS (select * from @tmp))
    		BEGIN
    			DECLARE @Ton float;
    			SET @Ton = @TonDauKy;
    			DECLARE @TonTienMat float;
    			SET @TonTienMat = @TonDauKy;
    			DECLARE @TonTienGui float;
    			SET @TonTienGui = @TonDauKy;
    			
    			DECLARE @TienThu float;
    			DECLARE @TienChi float;
    			DECLARE @ThuTienMat float;
    			DECLARE @ChiTienMat float;
    			DECLARE @ThuTienGui float;
    			DECLARE @ChiTienGui float;
    			DECLARE @TonLuyKe float;
    				DECLARE @ID_HoaDon UNIQUEIDENTIFIER;
    	DECLARE CS_ItemUpDate CURSOR SCROLL LOCAL FOR SELECT TienThu, TienChi, ThuTienGui, ThuTienMat, ChiTienGui, ChiTienMat, ID_HoaDon FROM @tmp ORDER BY NgayLapHoaDon
    	OPEN CS_ItemUpDate 
    FETCH FIRST FROM CS_ItemUpDate INTO @TienThu, @TienChi, @ThuTienGui, @ThuTienMat, @ChiTienGui, @ChiTienMat, @ID_HoaDon
    WHILE @@FETCH_STATUS = 0
    BEGIN
    	SET @Ton = @Ton + @TienThu - @TienChi;
    	SET @TonTienMat = @TonTienMat + @ThuTienMat - @ChiTienMat;
    	SET @TonTienGui = @TonTienGui + @ThuTienGui - @ChiTienGui;
    	UPDATE @tmp SET TonLuyKe = @Ton, TonLuyKeTienMat = @TonTienMat, TonLuyKeTienGui = @TonTienGui WHERE ID_HoaDon = @ID_HoaDon
    	FETCH NEXT FROM CS_ItemUpDate INTO @TienThu, @TienChi, @ThuTienGui, @ThuTienMat, @ChiTienGui, @ChiTienMat, @ID_HoaDon
    END
    CLOSE CS_ItemUpDate
    DEALLOCATE CS_ItemUpDate
    	END
    	ELSE
    	BEGIN
    		Insert INTO @tmp
    	SELECT '00000000-0000-0000-0000-000000000000', 'TRINH0001', '1989-04-07','','','0','0','0','0','0','0','0','0', @TonDauKy, @TonDauKy, @TonDauKy, '','','', '00000000-0000-0000-0000-000000000000', ''
    	END
    	Select 
    		ID_HoaDon,
    	MaPhieuThu,
    	NgayLapHoaDon,
    	KhoanMuc,
    	TenDoiTac,
    	@TonDauKy as TonDauKy,
    	TienMat,
    	TienGui,
    	TienThu,
    	TienChi,
    	ThuTienMat,
    	ChiTienMat,
    	ThuTienGui,
    	ChiTienGui,
    	TonLuyKe,
    	TonLuyKeTienMat,
    	TonLuyKeTienGui,
    	SoTaiKhoan, 
    	NganHang, 
    	GhiChu,
    		IDDonVi, TenDonVi
    	 from @tmp order by NgayLapHoaDon
END");

            Sql(@"ALTER PROCEDURE [dbo].[BaoCaoTongQuan_NhatKyHoatDong]
    @ID_DonVi [uniqueidentifier],
	@TongQuan_XemDS_PhongBan [varchar](max),
	@TongQuan_XemDS_HeThong [varchar](max),
	@ID_NguoiDung [uniqueidentifier]
AS
BEGIN

set nocount on;
	DECLARE @LaAdmin as nvarchar
    Set @LaAdmin = (Select nd.LaAdmin From HT_NguoiDung nd where nd.ID = @ID_NguoiDung)
	If (@LaAdmin = 1)
	BEGIN
		SELECT TOP(12)
		MAX(a.TenNhanVien) as TenNhanVien,
		a.MaHoaDon,
		CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien,
		MAX(a.NgayLapHoaDon) as NgayGoc,
    	CONVERT(VARCHAR, MAX(a.NgayLapHoaDon), 22) as NgayLapHoaDon,
		CASE 
			WHEN a.LoaiHoaDon = 1 then N'bán đơn hàng'  
			WHEN a.LoaiHoaDon = 3 then N'tạo báo giá' 
			WHEN a.LoaiHoaDon = 4 then N'nhập kho đơn hàng'  
			WHEN a.LoaiHoaDon = 6 then N'nhận hàng trả'  
			WHEN a.LoaiHoaDon = 7 then N'trả hàng nhà cung cấp'  
			WHEN a.LoaiHoaDon = 8 then N'xuất kho đơn hàng'  
			WHEN a.LoaiHoaDon = 25 then N'tạo hóa đơn sửa chữa'  
			Else N'bán gói dịch vụ'
		END as TenLoaiChungTu,
		a.LoaiHoaDon AS LoaiHoaDon
		FROM
		(
    		SELECT
			hdb.ID as ID_HoaDon,
			hdb.MaHoaDon,
			nv.TenNhanVien,
			hdb.LoaiHoaDon,
			hdb.NgayLapHoaDon,
    		isnull(hdb.PhaiThanhToan, hdb.TongThanhToan) as ThanhTien
    		FROM
    		BH_HoaDon hdb
			join NS_NhanVien nv on hdb.ID_NhanVien = nv.ID
    		where hdb.ID_DonVi = @ID_DonVi
    		and hdb.ChoThanhToan = 0
    		and hdb.LoaiHoaDon in (1,3,4,5,6,7,8,19, 25)
		) a
    	GROUP BY a.ID_HoaDon, a.LoaiHoaDon, a.MaHoaDon
		ORDER BY NgayGoc DESC
	END
	ELSE
	BEGIN
		if (@TongQuan_XemDS_HeThong = 'TongQuan_XemDS_HeThong')
		BEGIN
			SELECT TOP(12)
			MAX(a.TenNhanVien) as TenNhanVien,
			a.MaHoaDon,
			CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien,
			MAX(a.NgayLapHoaDon) as NgayGoc,
    		CONVERT(VARCHAR, MAX(a.NgayLapHoaDon), 22) as NgayLapHoaDon,
			CASE WHEN a.LoaiHoaDon = 1 then N'bán đơn hàng'  
			WHEN a.LoaiHoaDon = 3 then N'tạo báo giá' 
			WHEN a.LoaiHoaDon = 4 then N'nhập kho đơn hàng'  
			WHEN a.LoaiHoaDon = 6 then N'nhận hàng trả'  
			WHEN a.LoaiHoaDon = 7 then N'trả hàng nhà cung cấp'  
			WHEN a.LoaiHoaDon = 8 then N'xuất kho đơn hàng'  
			WHEN a.LoaiHoaDon = 25 then N'tạo hóa đơn sửa chữa'  
			Else N'bán gói dịch vụ'
			END as TenLoaiChungTu,
			a.LoaiHoaDon AS LoaiHoaDon
			FROM
			(
    			SELECT
				hdb.ID as ID_HoaDon,
				hdb.MaHoaDon,
				nv.TenNhanVien,
				hdb.LoaiHoaDon,
				hdb.NgayLapHoaDon,
    			isnull(hdb.PhaiThanhToan, hdb.TongThanhToan) as ThanhTien
    			FROM
    			BH_HoaDon hdb
    			
				join NS_NhanVien nv on hdb.ID_NhanVien = nv.ID
    			where hdb.ID_DonVi = @ID_DonVi
    			and hdb.ChoThanhToan = 0
    			and hdb.LoaiHoaDon in (1,3,4,5,6,7,8,19, 25)
			) a
    		GROUP BY a.ID_HoaDon, a.LoaiHoaDon, a.MaHoaDon
			ORDER BY NgayGoc DESC
		END 
		ELSE 
		BEGIN
			if (@TongQuan_XemDS_PhongBan = 'TongQuan_XemDS_PhongBan')
			BEGIN
				DECLARE @ID_NhanVienPhongBan table (ID_NhanVien uniqueidentifier);
				INSERT INTO @ID_NhanVienPhongBan exec getListID_NhanVienPhongBan @ID_NguoiDung;
				SELECT TOP(12)
				MAX(a.TenNhanVien) as TenNhanVien,
				a.MaHoaDon,
				CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien,
				MAX(a.NgayLapHoaDon) as NgayGoc,
    			CONVERT(VARCHAR, MAX(a.NgayLapHoaDon), 22) as NgayLapHoaDon,
				CASE WHEN a.LoaiHoaDon = 1 then N'bán đơn hàng'  
				WHEN a.LoaiHoaDon = 3 then N'tạo báo giá' 
				WHEN a.LoaiHoaDon = 4 then N'nhập kho đơn hàng'  
				WHEN a.LoaiHoaDon = 6 then N'nhận hàng trả'  
				WHEN a.LoaiHoaDon = 7 then N'trả hàng nhà cung cấp'  
				WHEN a.LoaiHoaDon = 8 then N'xuất kho đơn hàng'  
				WHEN a.LoaiHoaDon = 25 then N'tạo hóa đơn sửa chữa'  
				Else N'bán gói dịch vụ'
				END as TenLoaiChungTu,
				a.LoaiHoaDon AS LoaiHoaDon
				FROM
				(
    				SELECT
					hdb.ID as ID_HoaDon,
					hdb.MaHoaDon,
					nv.TenNhanVien,
					hdb.LoaiHoaDon,
					hdb.NgayLapHoaDon,
					isnull(hdb.PhaiThanhToan, hdb.TongThanhToan) as ThanhTien
    				FROM
    				BH_HoaDon hdb
					join NS_NhanVien nv on hdb.ID_NhanVien = nv.ID
					join @ID_NhanVienPhongBan pb on nv.ID = pb.ID_NhanVien
    				where hdb.ID_DonVi = @ID_DonVi
    				and hdb.ChoThanhToan = 0
    				and hdb.LoaiHoaDon in (1,3,4,5,6,7,8,19, 25)					
				) a
    			GROUP BY a.ID_HoaDon, a.LoaiHoaDon, a.MaHoaDon
				ORDER BY NgayGoc DESC
			END
			else 
			BEGIN
				DECLARE @ID_NhanVienDS table (ID_NhanVien uniqueidentifier);
				INSERT INTO @ID_NhanVienDS exec getListID_NhanVienDS @ID_NguoiDung;
				SELECT TOP(12)
				MAX(a.TenNhanVien) as TenNhanVien,
				a.MaHoaDon,
				CAST(ROUND(SUM(a.ThanhTien), 0) as float) as ThanhTien,
				MAX(a.NgayLapHoaDon) as NgayGoc,
    			CONVERT(VARCHAR, MAX(a.NgayLapHoaDon), 22) as NgayLapHoaDon,
				CASE WHEN a.LoaiHoaDon = 1 then N'bán đơn hàng'  
				WHEN a.LoaiHoaDon = 3 then N'tạo báo giá' 
				WHEN a.LoaiHoaDon = 4 then N'nhập kho đơn hàng'  
				WHEN a.LoaiHoaDon = 6 then N'nhận hàng trả'  
				WHEN a.LoaiHoaDon = 7 then N'trả hàng nhà cung cấp'  
				WHEN a.LoaiHoaDon = 8 then N'xuất kho đơn hàng'  
				WHEN a.LoaiHoaDon = 25 then N'tạo hóa đơn sửa chữa'  
				Else N'bán gói dịch vụ'
				END as TenLoaiChungTu,
				a.LoaiHoaDon AS LoaiHoaDon
				FROM
				(
    				SELECT
					hdb.ID as ID_HoaDon,
					hdb.MaHoaDon,
					nv.TenNhanVien,
					hdb.LoaiHoaDon,
					hdb.NgayLapHoaDon,
    				isnull(hdb.PhaiThanhToan, hdb.TongThanhToan) as ThanhTien
    				FROM
    				BH_HoaDon hdb   			
					join NS_NhanVien nv on hdb.ID_NhanVien = nv.ID
					join @ID_NhanVienDS pb on nv.ID = pb.ID_NhanVien
    				where hdb.ID_DonVi = @ID_DonVi
    				and hdb.ChoThanhToan = 0
    				and hdb.LoaiHoaDon in (1,3,4,5,6,7,8,19, 25)
				) a
    			GROUP BY a.ID_HoaDon, a.LoaiHoaDon, a.MaHoaDon
				ORDER BY NgayGoc DESC
			END
		END
	END
END");

            Sql(@"ALTER PROCEDURE [dbo].[BaoDuong_InsertListDetail_ByNhomHang]
    @ID_HangHoa [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @ID_NhomHangHoa uniqueidentifier, @QuanLyBaoDuong int , @LoaiBaoDuong int
    	select @ID_NhomHangHoa=  ID_NhomHang, @QuanLyBaoDuong= QuanLyBaoDuong, @LoaiBaoDuong = LoaiBaoDuong
    	from DM_HangHoa where id= @ID_HangHoa
    
    declare @tblNhom table(ID_NhomHang uniqueidentifier)
    	insert into @tblNhom
    	select ID from dbo.GetListNhomHangHoa(@ID_NhomHangHoa)
    
    	-- update quanlybaoduong for all hanghoa by nhom		
    	update hh set hh.QuanLyBaoDuong= @QuanLyBaoDuong , hh.LoaiBaoDuong= @LoaiBaoDuong
    	from DM_HangHoa hh 
    	where exists (
    	select id from @tblNhom nhom where hh.ID_NhomHang= nhom.ID_NhomHang)
		
    	---- get list hanghoa by nhomhang
    	select hh.ID, hh.TenHangHoa
    	into #temp
    	from DM_HangHoa hh
    	where hh.TheoDoi = 1
    	and hh.ID not like @ID_HangHoa
    	and exists (
    	select id from @tblNhom nhom where hh.ID_NhomHang= nhom.ID_NhomHang)
    
    	--- delete all by nhom
    	delete bd	
    	from DM_HangHoa_BaoDuongChiTiet bd
    	where exists (
    	select ID from #temp where bd.ID_HangHoa= #temp.ID
    	)
    
    	--- insert again
    	insert into DM_HangHoa_BaoDuongChiTiet
    	select NEWID(), tblhh.ID, a.LanBaoDuong, a.GiaTri, a.LoaiGiaTri, a.BaoDuongLapDinhKy
    	from #temp tblhh	
    	cross join 
    	(
    		select bd.LanBaoDuong, bd.GiaTri, bd.LoaiGiaTri, bd.BaoDuongLapDinhKy
    		from DM_HangHoa_BaoDuongChiTiet bd
    		where bd.ID_HangHoa= @ID_HangHoa
    	) a
END");

            Sql(@"ALTER PROCEDURE [dbo].[ChiTietTraHang_insertChietKhauNV]
    @ID_HoaDon [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    	insert into BH_NhanVienThucHien (ID, ID_NhanVien, ID_ChiTietHoaDon, TienChietKhau, TheoYeuCau, PT_ChietKhau, ThucHien_TuVan, 
		ID_HoaDon, TinhChietKhauTheo, HeSo, ID_QuyHoaDon, TinhHoaHongTruocCK)
    	select newid(), th.ID_NhanVien, cttra.ID, 
    	-- important: neu chiet khau theo VND --> khong nhan voi HeSo
    	case when th.PT_ChietKhau = 0 then case when ctmua.ThanhTien = 0 then 0 else (th.TienChietKhau / ctmua.ThanhTien ) * cttra.ThanhTien end
		else th.PT_ChietKhau/100 *th.HeSo * cttra.ThanhTien end as TienChietKhau,
    	th.TheoYeuCau, th.PT_ChietKhau, th.ThucHien_TuVan, null, th.TinhChietKhauTheo, th.HeSo, null, isnull(th.TinhHoaHongTruocCK,0)
    	from BH_NhanVienThucHien th
    	join BH_HoaDon_ChiTiet ctmua on th.ID_ChiTietHoaDon = ctmua.id
    	join BH_HoaDon_ChiTiet cttra on ctmua.ID= cttra.ID_ChiTietGoiDV
    	join BH_HoaDon hd on ctmua.ID_HoaDon= hd.ID
    	where hd.ID=@ID_HoaDon 
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetBangLuongChiTiet_ofNhanVien]
    @IDChiNhanhs [nvarchar](max),
    @IDNhanVien [uniqueidentifier],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs);
    
    	with data_cte
    		as(
    
    		select blct.*,
    			isnull(quyhd.DaTra,0) as DaTra ,
    			round(blct.LuongSauGiamTru - isnull(quyhd.DaTra,0),3) as ConLai
    		from
    		(select bl.NgayTao,
    				bl.TuNgay,
    				bl.DenNgay,
    				ct.ID as ID_BangLuong_ChiTiet,
    				ct.MaBangLuongChiTiet,
    				ct.NgayCongThuc,
    				ct.NgayCongChuan,
    				ct.LuongCoBan,
    				ct.TongLuongNhan as LuongChinh, 
    				ct.LuongOT,
    				ct.PhuCapCoBan,
    				ct.PhuCapKhac,
    				ct.KhenThuong,
    				ct.KyLuat,
    				ct.ChietKhau,    					
    				ct.TongLuongNhan +  ct.LuongOT + ct.PhuCapCoBan + ct.PhuCapKhac + ChietKhau  as LuongTruocGiamTru,
    				ct.TongTienPhat,
    			ct.LuongThucNhan as LuongSauGiamTru
    			
    		from NS_BangLuong_ChiTiet ct
    		join NS_BangLuong bl on ct.ID_BangLuong= bl.ID
    		where ct.ID_NhanVien= @IDNhanVien
    		and exists (select ID from @tblChiNhanh dv where bl.ID_DonVi = dv.ID)
    		and bl.TrangThai in (3,4)
    		) blct
    		left join ( select qct.ID_BangLuongChiTiet,
    							sum(qct.TienThu) +sum( isnull(qct.TruTamUngLuong,0)) as DaTra
    					 from Quy_HoaDon_ChiTiet qct 
    					 join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID 
    					 where qhd.TrangThai = 1
    					 and qct.ID_NhanVien= @IDNhanVien
    					 and exists (select ID from @tblChiNhanh dv where qhd.ID_DonVi = dv.ID)
    					 group by qct.ID_BangLuongChiTiet) quyhd on blct.ID_BangLuong_ChiTiet = quyhd.ID_BangLuongChiTiet		 
    
    	),
    	count_cte
    		as (
    			select count(ID_BangLuong_ChiTiet) as TotalRow,
    				CEILING(COUNT(ID_BangLuong_ChiTiet) / CAST(@PageSize as float ))  as TotalPage,
    				sum(NgayCongThuc) as TongNgayCongThuc,
    				sum(LuongChinh) as TongLuongChinh,
    				sum(LuongOT) as TongLuongOT,
    				sum(PhuCapCoBan) as TongPhuCapCoBan,
    				sum(PhuCapKhac) as TongPhuCapKhac,
    				sum(KhenThuong) as TongKhenThuong,
    				sum(KyLuat) as TongKyLuat,
    				sum(ChietKhau) as TongChietKhau,
    				sum(LuongTruocGiamTru) as TongLuongTruocGiamTru,
    				sum(TongTienPhat) as TongTienPhatAll,
    				sum(LuongSauGiamTru) as TongLuongSauGiamTru,
    				sum(DaTra) as TongDaTra,
    				sum(ConLai) as TongConLai
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaBangLuongChiTiet desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetListCustomer_byIDs]
    @IDCustomers [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT ID, MaDoiTuong, TenDoiTuong, DienThoai, Email , ISNULL(TongTichDiem,0) as TongTichDiem
	from DM_DoiTuong
	where exists (select Name from dbo.splitstring(@IDCustomers) where ID= Name)
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetListDatLich]
    @IdChiNhanhs [nvarchar](max),
    @ThoiGianFrom [datetime],
    @ThoiGianTo [datetime],
    @TrangThais [nvarchar](20),
    @TextSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
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
    -- Insert statements for procedure here
    	if(@PageSize != 0)
    	BEGIN
    		with data_cte
    	as
    	(
    	SELECT dl.Id, dl.ThoiGian, dl.IDDoiTuong AS IdKhachHang, ISNULL(dt.MaDoiTuong, '') AS MaKhachHang,
    	ISNULL(dt.DienThoai, dl.SoDienThoai) AS SoDienThoai, ISNULL(dt.TenDoiTuong, dl.TenDoiTuong) AS TenKhachHang, ISNULL(dt.DiaChi, dl.DiaChi) AS DiaChi,
    	ISNULL(dt.NgaySinh_NgayTLap, dl.NgaySinh) AS NgaySinh, dl.IDXe, ISNULL(xe.BienSo, dl.BienSo) AS BienSo, dl.LoaiXe AS MauXe, dl.TrangThai, 
		dv.TenDonVi AS TenChiNhanh, dv.ID AS IdDonVi, dv.MaDonVi AS MaChiNhanh
    	FROM CSKH_DatLich dl
    	INNER JOIN @tblDonVi donvi ON dl.IDDonVi = donvi.ID_DonVi
    	INNER JOIN DM_DonVi dv ON donvi.ID_DonVi = dv.ID
    	LEFT JOIN DM_DoiTuong dt ON dt.ID = dl.IDDoiTuong
    	LEFT JOIN Gara_DanhMucXe xe ON xe.ID = dl.IDXe
    	WHERE exists (select GiaTri from @tbTrangThai tt where dl.TrangThai = tt.GiaTri)
    	AND dl.ThoiGian BETWEEN @ThoiGianFrom AND @ThoiGianTo
    	AND ((select count(Name) from @tblSearch b where     			
    		dl.TenDoiTuong like '%'+b.Name+'%'
    		or dl.SoDienThoai like '%'+b.Name+'%'
    		or dl.DiaChi like '%'+b.Name+'%'		
    		or dl.BienSo like '%'+b.Name+'%'
    		or dl.LoaiXe like '%'+b.Name+'%'
    		or dl.NgaySinh like '%'+b.Name+'%'
    		or dv.TenDonVi like '%'+b.Name+'%'
    		)=@count or @count=0)
    			), count_cte
    		as
    		(
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
    			from data_cte
    		)
    		SELECT dt.*, ct.* FROM data_cte dt
    		CROSS JOIN count_cte ct
    		ORDER BY dt.ThoiGian desc
    		OFFSET (@CurrentPage * @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY;
    		END
    		ELSE
    		BEGIN
    	with data_cte
    	as
    	(
    	SELECT dl.Id, dl.ThoiGian, dl.IDDoiTuong AS IdKhachHang, ISNULL(dt.MaDoiTuong, '') AS MaKhachHang,
    	ISNULL(dt.DienThoai, dl.SoDienThoai) AS SoDienThoai, ISNULL(dt.TenDoiTuong, dl.TenDoiTuong) AS TenKhachHang, ISNULL(dt.DiaChi, dl.DiaChi) AS DiaChi,
    	ISNULL(dt.NgaySinh_NgayTLap, dl.NgaySinh) AS NgaySinh, dl.IDXe, ISNULL(xe.BienSo, dl.BienSo) AS BienSo, dl.LoaiXe AS MauXe, dl.TrangThai, 
		dv.TenDonVi AS TenChiNhanh, dv.ID AS IdDonVi, dv.MaDonVi AS MaChiNhanh
    	FROM CSKH_DatLich dl
    	INNER JOIN @tblDonVi donvi ON dl.IDDonVi = donvi.ID_DonVi
    	INNER JOIN DM_DonVi dv ON donvi.ID_DonVi = dv.ID
    	LEFT JOIN DM_DoiTuong dt ON dt.ID = dl.IDDoiTuong
    	LEFT JOIN Gara_DanhMucXe xe ON xe.ID = dl.IDXe
    	WHERE exists (select GiaTri from @tbTrangThai tt where dl.TrangThai = tt.GiaTri)
    	AND dl.ThoiGian BETWEEN @ThoiGianFrom AND @ThoiGianTo
    	AND ((select count(Name) from @tblSearch b where     			
    		dl.TenDoiTuong like '%'+b.Name+'%'
    		or dl.SoDienThoai like '%'+b.Name+'%'
    		or dl.DiaChi like '%'+b.Name+'%'		
    		or dl.BienSo like '%'+b.Name+'%'
    		or dl.LoaiXe like '%'+b.Name+'%'
    		or dl.NgaySinh like '%'+b.Name+'%'
    		or dv.TenDonVi like '%'+b.Name+'%'
    		)=@count or @count=0)
    			)
    			SELECT dt.*, 0 AS TotalRow, CAST(0 AS FLOAT) AS TotalPage FROM data_cte dt
    			ORDER BY dt.ThoiGian desc;
    			END
END");

            Sql(@"ALTER PROCEDURE [dbo].[getListHangHoaBy_IDNhomHang]
    @ID_NhomHang [nvarchar](max),
	@ID_DonVi [uniqueidentifier],
	@STT [int]
AS
BEGIN
    select 
		CAST(ROUND(ROW_NUMBER() over (order by dvqd.MaHangHoa), 0) + @STT as float) as SoThuTu,
    	dvqd.ID as ID_DonViQuiDoi,
		lh.ID as ID_LoHang,
    	dvqd.MaHangHoa,
		case when hh.QuanLyTheoLoHang is null then 'false' else hh.QuanLyTheoLoHang end as QuanLyTheoLoHang,
    	hh.TenHangHoa + dvqd.ThuocTinhGiaTri as TenHangHoaFull,
    	hh.TenHangHoa,
    	dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    	dvqd.TenDonViTinh,
		Case when lh.ID is null then '' else lh.MaLoHang end as TenLoHang,
		Case when lh.ID is null then '' else lh.NgaySanXuat end as NgaySanXuat,
		Case when lh.ID is null then '' else lh.NgayHetHan end as NgayHetHan,
    	Case when gv.ID is null then 0 else Cast(round(gv.GiaVon, 0) as float) end as GiaVonHienTai,
    	Case when gv.ID is null then 0 else Cast(round(gv.GiaVon, 0) as float) end as GiaVonMoi,
    	cast(0 as float)as GiaVonTang,
    	cast(0 as float) as GiaVonGiam
    	FROM 
    	DonViQuiDoi dvqd 
    	inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
		left join DM_LoHang lh on hh.ID = lh.ID_HangHoa
		left join DM_GiaVon gv on (dvqd.ID = gv.ID_DonViQuiDoi and (lh.ID = gv.ID_LoHang or gv.ID_LoHang is null) and gv.ID_DonVi = @ID_DonVi)
    	where hh.ID_NhomHang in (select * from splitstring(@ID_NhomHang))
    		and dvqd.Xoa = '0'
			and dvqd.LaDonViChuan = 1
    		and hh.TheoDoi = 1
END");

            CreateStoredProcedure(name: "[dbo].[GetListImgInvoice_byCus]", parametersAction: p => new
            {
                TextSearch = p.String(),
                ID_Customer = p.String(),
                CurrentPage = p.Int(),
                PageSize = p.Int()
            }, body: @"SET NOCOUNT ON;

	if @TextSearch is null set @TextSearch='%%'
	else set @TextSearch = CONCAT(N'%',@TextSearch,'%');

	---- get listhoadon
	select distinct hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.DienGiai as GhiChu
	into #tblHD
		from BH_HoaDon_Anh anh
		join BH_HoaDon hd on hd.ID= anh.IdHoaDon
		where hd.ID_DoiTuong like @ID_Customer
		and hd.ChoThanhToan='0'
		and 
			(hd.MaHoaDon like @TextSearch
			or hd.DienGiai like @TextSearch);

		

	---- get list gdv
	select ctsd.*, gdv.MaHoaDon
	into #tblGDV
	from
	(
		select ctsd.ID_ChiTietGoiDV, ctsd.ID_HoaDon
		from BH_HoaDon_ChiTiet ctsd	
		where exists(select id from #tblHD hd where ctsd.ID_HoaDon = hd.ID)
	) ctsd
	join BH_HoaDon_ChiTiet ctm on ctsd.ID_ChiTietGoiDV= ctm.ID
	join BH_HoaDon gdv on ctm.ID_HoaDon= gdv.ID;


		with data_cte
		as					
		(
			select distinct hdsd.*, gdv.MaHoaDon as MaPhieuThu ---- muon tam truong
			from #tblHD hdsd
			left join #tblGDV gdv on hdsd.ID= gdv.ID_HoaDon			
	   ),
	   count_cte
	   as
	   (
			select 
				count(ID) as TotalRow,
				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage
			from data_cte
	   )
		select dt.*, cte.*
		from data_cte dt
		cross join count_cte cte
		order by dt.NgayLapHoaDon desc
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY; ");

            Sql(@"ALTER PROCEDURE [dbo].[GetListTheGiaTri]
    @IDDonVis [nvarchar](max),
    @TextSearch [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @TrangThais [nvarchar](10),
    @MucNapFrom [float],
    @MucNapTo [float],
    @KhuyenMaiFrom [float],
    @KhuyenMaiTo [float],
    @KhuyenMaiLaPTram [int],
    @ChietKhauFrom [float],
    @ChietKhauTo [float],
    @ChietKhauLaPTram [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	declare @MucNapMax float= (select max(TongChiPhi) from BH_HoaDon where ChoThanhToan= 0 and LoaiHoaDon= 22 );
    	if @MucNapTo is null
    		set @MucNapTo= @MucNapMax
    	if @KhuyenMaiTo is null
    		set @KhuyenMaiTo = @MucNapMax
    	if @ChietKhauTo is null
    		set @ChietKhauTo= @MucNapMax;
    	
    	with data_cte
    	as
    	(
    
    	select tblThe.ID,tblThe.MaHoaDon,tblThe.NgayLapHoaDon,tblThe.NgayTao,
    		tblThe.TongChiPhi as MucNap,
    		tblThe.TongChietKhau as KhuyenMaiVND,
    		tblThe.TongTienHang as TongTienNap,
    		tblThe.TongTienThue as SoDuSauNap,
    		tblThe.TongGiamGia as ChietKhauVND,
    		ISNULL(tblThe.DienGiai,'') as GhiChu,
    		tblThe.NguoiTao,
    		ISNULL(tblThe.ID_DoiTuong,'00000000-0000-0000-0000-000000000000') as ID_DoiTuong,
    		tblThe.PhaiThanhToan,
    		ISNULL(tblThe.NhanVienThucHien,'') as NhanVienThucHien,
    		tblThe.MaDoiTuong as MaKhachHang,
    		tblThe.TenDoiTuong as TenKhachHang,
    		tblThe.DienThoai as SoDienThoai,
    		tblThe.DiaChi as DiaChiKhachHang,
    		tblThe.ChoThanhToan,
    		tblThe.ChietKhauPT,
    		tblThe.KhuyenMaiPT,
			tblThe.ID_DonVi,
    		ISNULL(soquy.TienMat,0) as TienMat,
    		ISNULL(soquy.TienPOS,0) as TienATM,
    		ISNULL(soquy.TienCK,0) as TienGui,
    		ISNULL(soquy.TienThu,0) as KhachDaTra,
    		dv.TenDonVi,
    		dv.SoDienThoai as DienThoaiChiNhanh,
    		dv.DiaChi as DiaChiChiNhanh
    	from
    		(
    		select *
    		from
    			(select hd.*, 
						iif(hd.TongChiPhi=0,0, hd.TongGiamGia/hd.TongChiPhi * 100) as ChietKhauPT,
						iif(hd.TongChiPhi=0,0, hd.TongChietKhau/hd.TongChiPhi * 100) as KhuyenMaiPT,
    					dt.MaDoiTuong, dt.TenDoiTuong,
    					dt.DienThoai, 
    					dt.DiaChi,
    					case when hd.ChoThanhToan is null then '10' else '12' end as TrangThai,
    					NhanVienThucHien
    				from BH_HoaDon hd
    				join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    				left join (
    						Select distinct
    							(
    								Select distinct nv.TenNhanVien + ',' AS [text()]
    								From dbo.BH_NhanVienThucHien th
    								join dbo.NS_NhanVien nv on th.ID_NhanVien = nv.ID
    								where th.ID_HoaDon= nvth.ID_HoaDon
    								For XML PATH ('')
    							) NhanVienThucHien, nvth.ID_HoaDon
    							From dbo.BH_NhanVienThucHien nvth
    							) nvThucHien on hd.ID = nvThucHien.ID_HoaDon
    				where exists (select name from dbo.splitstring(@IDDonVis) dv where hd.ID_DonVi= dv.Name)	
    				and hd.LoaiHoaDon = 22
    				and hd.TongChiPhi >= @MucNapFrom and hd.TongChiPhi <= @MucNapTo -- mucnap
    				and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon <=@ToDate
    					AND ((select count(Name) from @tblSearchString b where 
    					dt.MaDoiTuong like '%'+b.Name+'%' 
    					or dt.TenDoiTuong like '%'+b.Name+'%' 
    						or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%' 
    					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    						or dt.DienThoai like '%'+b.Name+'%'			
    					or hd.MaHoaDon like '%' +b.Name +'%' 
    						or hd.NguoiTao like '%' +b.Name +'%' 				
    						)=@count or @count=0)	
    			) the
    			where IIF(@KhuyenMaiLaPTram = 1, the.TongChietKhau, the.KhuyenMaiPT) >= @KhuyenMaiFrom -- khuyenmai
    				and IIF(@KhuyenMaiLaPTram = 1, the.TongChietKhau, the.KhuyenMaiPT) <= @KhuyenMaiTo
    				and IIF(@ChietKhauLaPTram = 1, the.TongGiamGia, the.ChietKhauPT) >= @ChietKhauFrom -- giamgia
    				and IIF(@ChietKhauLaPTram = 1, the.TongGiamGia, the.ChietKhauPT) <= @ChietKhauTo
    				and the.TrangThai like @TrangThais 
    		) tblThe		
    	join DM_DonVi dv on tblThe.ID_DonVi= dv.ID
    	left join ( select quy.ID_HoaDonLienQuan, 
    					sum(quy.TienThu) as TienThu,
    					sum(quy.TienMat) as TienMat,
    					sum(quy.TienPOS) as TienPOS,
    					sum(quy.TienCK) as TienCK
    				from
    				(
    					select qct.ID_HoaDonLienQuan,
    						qct.TienMat,
    						qct.TienThu,
    						case when tk.TaiKhoanPOS = '1' then qct.TienGui else 0 end as TienPOS,
    						case when tk.TaiKhoanPOS = '0' then qct.TienGui else 0 end as TienCK
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang= tk.ID
    					where qhd.TrangThai= 1 or qhd.TrangThai is null
    				) quy 
    				group by quy.ID_HoaDonLienQuan) soquy on tblThe.ID= soquy.ID_HoaDonLienQuan
    	),
    	count_cte
    	as (
    		select count(ID) as TotalRow,
    			CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    			sum(MucNap) as TongMucNapAll,
    			sum(KhuyenMaiVND) as TongKhuyenMaiAll,
    			sum(TongTienNap) as TongTienNapAll,			
    			sum(ChietKhauVND) as TongChietKhauAll,
    			sum(SoDuSauNap) as SoDuSauNapAll,
    			sum(PhaiThanhToan) as PhaiThanhToanAll,			
    			sum(TienMat) as TienMatAll,
    			sum(TienATM) as TienATMAll,
    			sum(TienGui) as TienGuiAll,
    			sum(KhachDaTra) as KhachDaTraAll
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.NgayLapHoaDon desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetLuongOT_ofNhanVien]
    @IDChiNhanhs [uniqueidentifier],
    @IDNhanViens [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @NgayCongChuan [int]
AS
BEGIN
    SET NOCOUNT ON;	
    
    		declare @tblCongThuCong CongThuCong
    		insert into @tblCongThuCong
    		exec dbo.GetChiTietCongThuCong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    		declare @tblThietLapLuong ThietLapLuong
    		insert into @tblThietLapLuong
    		exec GetNS_ThietLapLuong @IDChiNhanhs,@IDNhanViens, @FromDate, @ToDate
    
    
    		-- ============= OT Ngay ====================
    		declare @thietlapOTNgay table (ID_NhanVien uniqueidentifier, ID uniqueidentifier, TenLoaiLuong nvarchar(max), LoaiLuong int, LuongCoBan float,  HeSo int, NgayApDung datetime, NgayKetThuc datetime)
    		insert into @thietlapOTNgay
    		select *	
    		from @tblThietLapLuong pc 		
    		where pc.LoaiLuong = 2
    
    		select  a.*,		
    				case when LaPhanTram = 0 then GiaTri else SoTien/@NgayCongChuan/8 end as Luong1GioCongCoBan ,
    				case when LaPhanTram = 0 then HeSo * GiaTri else (SoTien/@NgayCongChuan/8) * GiaTri * HeSo/100 end as Luong1GioCongQuyDoi				
    			into #temp1					
    			from
    			(
    			select bs.ID_CaLamViec, bs.TenCa, bs.TongGioCong1Ca, bs.ID_NhanVien,
    					bs.NgayCham, bs.LoaiNgay, bs.KyHieuCong, bs.SoGioOT, bs.Thu,
    					pc.SoTien,
    					pc.LoaiLuong,
    					pc.HeSo,

						case when ngayle.ID is null then -- 0.chunhat, 6.thu7, 1.ngaynghi, 2.ngayle, 3.ngaythuong  			
							case bs.Thu
								when 6 then 6
								when 0 then 0
							else 3 end
						else bs.LoaiNgay end LoaiNgayThuong_Nghi,	

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_GiaTri
								when 0 then tlct.ThCN_GiaTri
							else tlct.LuongNgayThuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_GiaTri
								when 2 then  tlct.NgayLe_GiaTri
							else tlct.LuongNgayThuong end
						end as GiaTri,

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_LaPhanTramLuong
								when 0 then tlct.CN_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_LaPhanTramLuong
								when 2 then  tlct.NgayLe_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						end as LaPhanTram								
    			from @tblCongThuCong bs
    			join NS_Luong_PhuCap pc  on bs.ID_NhanVien= pc.ID_NhanVien
    			join NS_ThietLapLuongChiTiet tlct on pc.ID= tlct.ID_LuongPhuCap 
				left join NS_NgayNghiLe ngayle on bs.NgayCham = ngayle.Ngay and ngayle.TrangThai='1'
    			where tlct.LaOT= 1 and pc.LoaiLuong= 2
				and exists (select tl.ID_PhuCap from @tblThietLapLuong tl where pc.ID = tl.ID_PhuCap)
    			) a			
    
    			declare @tblCongOTNgay table (ID_PhuCap uniqueidentifier, ID_NhanVien uniqueidentifier, LoaiLuong int, 
    			Luong1GioCongCoBan float, Luong1GioCongQuyDoi float, HeSoLuong int,NgayApDung datetime, NgayKetThuc datetime,
    			LoaiNgayThuong_Nghi int, LaPhanTram int,
    			SoGioOT float, LuongOT float, NgayCham datetime)	
    
    			declare @otngayID_NhanVien uniqueidentifier, @otngayID_PhuCap uniqueidentifier, @otngayTenLoaiLuong nvarchar(max), 
    			@otngayLoaiLuong int,@otngayLuongCoBan float, @otngayHeSo int, @otngayNgayApDung datetime, @otngayNgayKetThuc datetime
    
    			DECLARE curLuong CURSOR SCROLL LOCAL FOR
    		select *
    		from @thietlapOTNgay
    		OPEN curLuong -- cur 1
    	FETCH FIRST FROM curLuong
    	INTO @otngayID_NhanVien, @otngayID_PhuCap, @otngayTenLoaiLuong, @otngayLoaiLuong, @otngayLuongCoBan, @otngayHeSo, @otngayNgayApDung, @otngayNgayKetThuc
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
    				insert into @tblCongOTNgay
    				select @otngayID_PhuCap,@otngayID_NhanVien, @otngayLoaiLuong,tmp.Luong1GioCongCoBan,tmp.Luong1GioCongQuyDoi, tmp.GiaTri,@otngayNgayApDung, @otngayNgayKetThuc,
    					tmp.LoaiNgayThuong_Nghi, tmp.LaPhanTram,
    					tmp.SoGioOT,
    					tmp.SoGioOT * Luong1GioCongQuyDoi as LuongOT,
    					tmp.NgayCham
    				from #temp1 tmp
    				where tmp.ID_NhanVien = @otngayID_NhanVien and tmp.NgayCham >= @otngayNgayApDung and (@otngayNgayKetThuc is null OR tmp.NgayCham <= @otngayNgayKetThuc )  								
    				FETCH NEXT FROM curLuong 
    				INTO @otngayID_NhanVien, @otngayID_PhuCap, @otngayTenLoaiLuong, @otngayLoaiLuong, @otngayLuongCoBan, @otngayHeSo, @otngayNgayApDung, @otngayNgayKetThuc
    			END;
    			CLOSE curLuong  
    		DEALLOCATE curLuong 	
    
    
    			-- ============= OT Ca =================
    
    			declare @thietlapOTCa table (ID_NhanVien uniqueidentifier, ID uniqueidentifier, TenLoaiLuong nvarchar(max), LoaiLuong int, LuongCoBan float,  HeSo int, NgayApDung datetime, NgayKetThuc datetime)
    			insert into @thietlapOTCa
    			select *	
    			from @tblThietLapLuong pc 		
    			where pc.LoaiLuong = 3
    
    			select  a.*,	
    				case when LaPhanTram = 0 then GiaTri else case when LuongTheoCa is null then SoTien/TongGioCong1Ca else LuongTheoCa/TongGioCong1Ca end end Luong1GioCongCoBan,
    				case when LaPhanTram = 0 then GiaTri else case when LuongTheoCa is null then SoTien/TongGioCong1Ca * GiaTri/100 
    				else LuongTheoCa/TongGioCong1Ca* GiaTri/100 end end as Luong1GioCongQuyDoi				
    			into #temp2					
    			from
    				(select bs.ID_CaLamViec, bs.TenCa, bs.TongGioCong1Ca, bs.ID_NhanVien,
    					bs.NgayCham, bs.LoaiNgay, bs.KyHieuCong, bs.SoGioOT, bs.Thu,
    					pc.SoTien,
    					theoca.LuongTheoCa,
    					pc.LoaiLuong,
    					pc.HeSo,
						case when ngayle.ID is null then -- 0.chunhat, 6.thu7, 1.ngaynghi, 2.ngayle, 3.ngaythuong  			
							case bs.Thu
								when 6 then 6
								when 0 then 0
							else 3 end
						else bs.LoaiNgay end LoaiNgayThuong_Nghi,	

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_GiaTri
								when 0 then tlct.ThCN_GiaTri
							else tlct.LuongNgayThuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_GiaTri
								when 2 then  tlct.NgayLe_GiaTri
							else tlct.LuongNgayThuong end
						end as GiaTri,

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_LaPhanTramLuong
								when 0 then tlct.CN_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_LaPhanTramLuong
								when 2 then  tlct.NgayLe_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						end as LaPhanTram
    				
    				from @tblCongThuCong bs
    				join NS_Luong_PhuCap pc  on bs.ID_NhanVien= pc.ID_NhanVien
    				join NS_ThietLapLuongChiTiet tlct on pc.ID= tlct.ID_LuongPhuCap 
					left join NS_NgayNghiLe ngayle on bs.NgayCham = ngayle.Ngay and ngayle.TrangThai='1'
    				left join (select tlca.LuongNgayThuong as LuongTheoCa, tlca.ID_CaLamViec, pca.ID_NhanVien
    						from NS_Luong_PhuCap pca
    						join NS_ThietLapLuongChiTiet tlca on pca.ID= tlca.ID_LuongPhuCap 
    						where tlca.LaOT= 0
    						) theoca on pc.ID_NhanVien= theoca.ID_NhanVien and bs.ID_CaLamViec= theoca.ID_CaLamViec
    				where tlct.LaOT= 1
    				and pc.LoaiLuong = 3
					and exists (select tl.ID_PhuCap from @tblThietLapLuong tl where pc.ID = tl.ID_PhuCap)
    				) a			
    
    		declare @tblCongOTCa table (ID_PhuCap uniqueidentifier, ID_NhanVien uniqueidentifier, LoaiLuong int, 
    		Luong1GioCongCoBan float, Luong1GioCongQuyDoi float, HeSoLuong int,NgayApDung datetime, NgayKetThuc datetime,
    		LoaiNgayThuong_Nghi int, LaPhanTram int,
    		ID_CaLamViec uniqueidentifier, TenCa nvarchar(100), TongGioCong1Ca float, 
    		SoGioOT float, LuongOT float, NgayCham datetime)				
    	
    		-- biến để đọc gtrị cursor
    		declare @ID_NhanVien uniqueidentifier, @ID_PhuCap uniqueidentifier, @TenLoaiLuong nvarchar(max), @LoaiLuong int,@LuongCoBan float, @HeSo int, @NgayApDung datetime, @NgayKetThuc datetime
    
    		DECLARE curLuong CURSOR SCROLL LOCAL FOR
    		select *
    		from @thietlapOTCa
    		OPEN curLuong
    	FETCH FIRST FROM curLuong
    	INTO @ID_NhanVien, @ID_PhuCap, @TenLoaiLuong, @LoaiLuong, @LuongCoBan, @HeSo, @NgayApDung, @NgayKetThuc
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
    				insert into @tblCongOTCa
    				select @ID_PhuCap,@ID_NhanVien, @LoaiLuong,tmp.Luong1GioCongCoBan,tmp.Luong1GioCongQuyDoi, tmp.GiaTri,@NgayApDung, @NgayKetThuc,
    				tmp.LoaiNgayThuong_Nghi, tmp.LaPhanTram,
    					tmp.ID_CaLamViec, 					
    					tmp.TenCa, 
    					tmp.TongGioCong1Ca,
    					tmp.SoGioOT,
    					tmp.SoGioOT * Luong1GioCongQuyDoi as LuongOT,
    					tmp.NgayCham
    				from #temp2 tmp
    				where tmp.ID_NhanVien = @ID_NhanVien and tmp.NgayCham >= @NgayApDung and (@NgayKetThuc is null OR tmp.NgayCham <= @NgayKetThuc )  								
    				FETCH NEXT FROM curLuong 
    				INTO @ID_NhanVien, @ID_PhuCap, @TenLoaiLuong, @LoaiLuong, @LuongCoBan, @HeSo, @NgayApDung, @NgayKetThuc
    			END;
    			CLOSE curLuong  
    		DEALLOCATE curLuong 	
    
    			select nv.MaNhanVien, nv.TenNhanVien, ot.ID_NhanVien,
    				LoaiLuong, Luong1GioCongCoBan, 
    				FORMAT(ot.Luong1GioCongQuyDoi,'###,###.###') as Luong1GioCongQuyDoi, 
    				HeSoLuong,
    				LoaiNgayThuong_Nghi, 
    				LaPhanTram,
    				ID_CaLamViec, TenCa, TongGioCong1Ca,
    				cast(SoGioOT as float) as SoGioOT,
    				FORMAT(ot.LuongOT,'###,###.###') as ThanhTien
    			from 
    				(
    				select 
    					ID_NhanVien, LoaiLuong, Luong1GioCongCoBan, Luong1GioCongQuyDoi, HeSoLuong,LoaiNgayThuong_Nghi, LaPhanTram,
    					ID_CaLamViec, TenCa, TongGioCong1Ca,
    					sum(SoGioOT) as SoGioOT,
    					sum(LuongOT) as LuongOT
    				from
    					(select ID_NhanVien, LoaiLuong, Luong1GioCongCoBan, Luong1GioCongQuyDoi, HeSoLuong,LoaiNgayThuong_Nghi, LaPhanTram,
    						'00000000-0000-0000-0000-000000000000' as ID_CaLamViec, '' as TenCa, 8 as TongGioCong1Ca,
    						SoGioOT,
    						LuongOT
    					from @tblCongOTNgay cong				
    
    					union all
    
    					select ID_NhanVien, LoaiLuong, Luong1GioCongCoBan, Luong1GioCongQuyDoi, HeSoLuong,LoaiNgayThuong_Nghi, LaPhanTram,
    						ID_CaLamViec, TenCa,TongGioCong1Ca,
    						SoGioOT,
    						LuongOT
    					from @tblCongOTCa cong	
    					) luongot
    				group by luongot.ID_NhanVien,LoaiLuong, Luong1GioCongCoBan,Luong1GioCongQuyDoi, HeSoLuong,LoaiNgayThuong_Nghi, LaPhanTram,
    						luongot.ID_CaLamViec, TenCa, TongGioCong1Ca
    			)ot
    			join NS_NhanVien nv on ot.ID_NhanVien= nv.ID
    			where ot.LuongOT > 0
    			order by ID_NhanVien, ID_CaLamViec
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetNhatKyTienCoc_OfDoiTuong]
    @ID_DoiTuong [nvarchar](max),
    @IDDonVis [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	
    	declare @tblChiNhanh table(ID_DonVi uniqueidentifier)
    	insert into @tblChiNhanh
    	select name from dbo.splitstring(@IDDonVis)
    
    	declare @LoaiDoiTuong int = (select LoaiDoiTuong from DM_DoiTuong where ID= @ID_DoiTuong)
    	if @FromDate is null
    		set @FromDate ='2020-01-01'
    	if @ToDate is null
    		set @ToDate = DATEADD(DAY,1,GETDATE())
    	
    
    	declare @tblDiary table(
    	 ID_PhieuThu uniqueidentifier, MaPhieuThu nvarchar(max), NgayLapPhieuThu datetime,
    		  ID_HoaDon uniqueidentifier, MaHoaDon nvarchar(max), GiaTri float, sLoaiHoaDon nvarchar(max), 
			  LoaiHoaDon int, LoaiThanhToan int,SoDu float
    	)
    
    	-- phieu naptiencoc
    	insert into @tblDiary
    	select * ,0
    	from
    	(
		
    	select
    		hd.ID as ID_PhieuThu,
    		hd.MaHoaDon as MaPhieuThu,
    		hd.NgayLapHoaDon,
    		null as ID,
    		'' as MaHoaDon,
    		case when @LoaiDoiTuong= 2 then iif(hd.LoaiHoaDon=11,-hd.TongTienThu, hd.TongTienThu)
    		else iif(hd.LoaiHoaDon=11,hd.TongTienThu, -hd.TongTienThu) end as GiaTri,
    		case when hd.LoaiHoaDon= 11 then
    			case when @LoaiDoiTuong= 2 then N'Chi trả cọc' else N'Nạp tiền cọc' end
    		else
    			case when @LoaiDoiTuong= 2 then N'Nạp tiền cọc' else N'Chi trả cọc' end
    		end as sLoaiHoaDon,
			hd.LoaiHoaDon,
			1 as LoaiThanhToan
    	from Quy_HoaDon hd
    	join Quy_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
    	where ct.ID_DoiTuong like @ID_DoiTuong
    	and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    	and hd.TrangThai='1'
    	and ct.LoaiThanhToan= 1
    	and exists(select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
		group by hd.ID ,hd.LoaiHoaDon,
    		hd.MaHoaDon,
    		hd.NgayLapHoaDon,
			hd.TongTienThu

    	union all
    	-- sudung coc
    	select
    		hd.ID as ID_PhieuThu,
    		hd.MaHoaDon as MaPhieuThu, 
    		hd.NgayLapHoaDon, 
    		hdsd.ID,
    		hdsd.MaHoaDon,
    		-sum(ct.TienThu) as GiaTri,
    		case hdsd.LoaiHoaDon
    			when 1 then N'Hóa đơn bán'
    			when 4 then N'Nhập hàng'
    			when 6 then N'Trả hàng'
    			when 7 then N'Trả hàng nhạp'
    			when 19 then N'Gói dịch vụ'
    			when 25 then N'Hóa đơn sửa chữa'
    	else '' end as sLoaiHoaDon,
		hd.LoaiHoaDon,
		0 as LoaiThanhToan
    	from Quy_HoaDon hd
    	join Quy_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
    	join BH_HoaDon hdsd on ct.ID_HoaDonLienQuan = hdsd.ID
    	where ct.ID_DoiTuong like @ID_DoiTuong
    	and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    	and exists(select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
    	and hd.TrangThai='1'
    	and ct.HinhThucThanhToan= 6
    	group by hd.ID ,
    		hd.MaHoaDon ,
			hd.LoaiHoaDon,
    		hd.NgayLapHoaDon, 
    		hdsd.ID,
    		hdsd.MaHoaDon,
    		hdsd.LoaiHoaDon
    	) a 
    	
    	declare @ID_PhieuThu uniqueidentifier, @MaPhieuThu nvarchar(max), @NgayLapPhieuThu datetime,
    		 @ID_HoaDon uniqueidentifier, @MaHoaDon nvarchar(max), @GiaTri float, @sLoaiHoaDon nvarchar(max), @SoDu float, @SoDuSauPhatSinh float
    	
    	set @SoDuSauPhatSinh =0
    	declare _cur cursor
    	for
    	select ID_PhieuThu, MaPhieuThu, NgayLapPhieuThu, ID_HoaDon, MaHoaDon,
			GiaTri, sLoaiHoaDon, SoDu
    	from @tblDiary tmp
    	order by NgayLapPhieuThu 
    	open _cur
    	fetch next from _cur 
    	into @ID_PhieuThu ,@MaPhieuThu , @NgayLapPhieuThu,
    		  @ID_HoaDon, @MaHoaDon , @GiaTri, @sLoaiHoaDon , @SoDu
    	while @@FETCH_STATUS =0
    		begin							
    			set @SoDuSauPhatSinh = @SoDuSauPhatSinh +  @GiaTri
    			update @tblDiary set SoDu = @SoDuSauPhatSinh where ID_PhieuThu = @ID_PhieuThu
    			
    			FETCH NEXT FROM _cur 
    			INTO @ID_PhieuThu ,@MaPhieuThu , @NgayLapPhieuThu,  @ID_HoaDon, @MaHoaDon , @GiaTri, @sLoaiHoaDon , @SoDu 		 
    		end  
    
    		close _cur
    		deallocate _cur;
    
    		with data_cte
    		as
    		(
    		select * 
			from @tblDiary
    		),
    		count_cte
    		as (
    			select count(ID_PhieuThu) as TotalRow,
    				CEILING(COUNT(ID_PhieuThu) / CAST(@PageSize as float ))  as TotalPage    				
    			from data_cte
    		)
    			-- do return class SoQuyDTO at C#
    		select 
    				dt.ID_PhieuThu as ID,
    				dt.MaPhieuThu as MaPhieuChi,
    				dt.NgayLapPhieuThu as NgayLapHoaDon,
    				dt.MaHoaDon,
    				dt.ID_HoaDon as ID_HoaDonGoc,
    				dt.GiaTri as PhaiThanhToan,
    				dt.SoDu as DuNoKH,
    				dt.sLoaiHoaDon as strLoaiHoaDon,		
					dt.LoaiHoaDon,
					dt.LoaiThanhToan,
    				cte.*
    		from data_cte dt    		
    		cross join count_cte cte
    		order by dt.NgayLapPhieuThu desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

            Sql(@"ALTER PROCEDURE [dbo].[HuyPhieuThu_UpdateCongNoTamUngLuong]
    @ID_ChiNhanh [uniqueidentifier],
    @IDQuyChiTiets [nvarchar](max),
    @LaPhieuTamUng [bit]
AS
BEGIN
    SET NOCOUNT ON;
    
    		declare @tblQuyChiTiet table(ID_QuyChiTiet uniqueidentifier)
    	insert into @tblQuyChiTiet
    	select Name from dbo.splitstring(@IDQuyChiTiets)
    
    		if @LaPhieuTamUng ='1'
    			begin
    			declare @sotienTamUng float, @nvTamUng uniqueidentifier, @idKhoanThuChi uniqueidentifier
    			-- get sotien, nhanvien tamung
    			select @sotienTamUng = TienThu, @nvTamUng= ID_NhanVien, @idKhoanThuChi= ID_KhoanThuChi
    			from Quy_HoaDon_ChiTiet qct1 where exists (select ID from @tblQuyChiTiet qct2 where qct1.ID= qct2.ID_QuyChiTiet)
					
				-- update khoanthuchi: tamungluong --> khoan khac
				-- or: huy phieu tamungluong
    			declare @giamtruLuong bit= (
    				select TinhLuong
    				from Quy_KhoanThuChi khoan
    				where id= @idKhoanThuChi)
		
    				update NS_CongNoTamUngLuong set CongNo = CongNo - @sotienTamUng where ID_NhanVien = @nvTamUng and ID_DonVi= @ID_ChiNhanh	
    			
    		end
    		else
    				update tblNoCu set CongNo= tblNoHienTai.NoHienTai
    			from NS_CongNoTamUngLuong tblNoCu
    			join (
    				select congno.ID,congno.CongNo + quy.TruTamUngLuong as NoHienTai
    				from NS_CongNoTamUngLuong congno
    				join (
    					select qct.ID_NhanVien, sum(qct.TruTamUngLuong) as TruTamUngLuong --- thanhtoan nhieulan
    					from Quy_HoaDon_ChiTiet qct 
    					join @tblQuyChiTiet qct2 on qct.ID= qct2.ID_QuyChiTiet		
						group by qct.ID_NhanVien
    					) quy on congno.ID_NhanVien= quy.ID_NhanVien
    				where congno.ID_DonVi= @ID_ChiNhanh
    			) tblNoHienTai on tblNoCu.ID= tblNoHienTai.ID
END");

            CreateStoredProcedure(name: "[dbo].[HuyPhieuThu_UpdateTrangThaiCong]", parametersAction: p => new
            {
                ID_QuyChiTiet = p.Guid()
            }, body: @"SET NOCOUNT ON;

	declare @ID_BangLuong_ChiTiet uniqueidentifier = (select top 1 ID_BangLuongChiTiet from Quy_HoaDon_ChiTiet where id = @ID_QuyChiTiet)

	

	if @ID_BangLuong_ChiTiet is not null
	begin

		select qct.ID_BangLuongChiTiet,
			sum(iif(qhd.TrangThai ='0',0, qct.TienThu + isnull(qct.TruTamUngLuong,0))) as DaThanhToan
		into #tblSQuy
		from Quy_HoaDon_ChiTiet qct 
		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
		where qct.ID_BangLuongChiTiet = @ID_BangLuong_ChiTiet
		group by qct.ID_BangLuongChiTiet

		update bs set TrangThai = iif(soquy.DaThanhToan > 0 ,4,3)    					   
		from NS_CongBoSung bs
		join #tblSQuy soquy on bs.ID_BangLuongChiTiet = soquy.ID_BangLuongChiTiet
	end");

            Sql(@"ALTER PROCEDURE [dbo].[import_DanhMucHangHoa]
	@isUpdateHang int,---1.update, 0.insert
	@isUpdateTonKho int,--- 1.no, 2.yes
	@ID_DonVi [uniqueidentifier],
    @ID_HangHoa [uniqueidentifier],
    @ID_DonViQuiDoi [uniqueidentifier],  
    @TenNhomHangHoaCha [nvarchar](max),
    @TenNhomHangHoaCha_KhongDau [nvarchar](max),
    @TenNhomHangHoaCha_KyTuDau [nvarchar](max),
    @MaNhomHangHoaCha [nvarchar](max), 
    @TenNhomHangHoa [nvarchar](max),
    @TenNhomHangHoa_KhongDau [nvarchar](max),
    @TenNhomHangHoa_KyTuDau [nvarchar](max),
    @MaNhomHangHoa [nvarchar](max),  
    @LoaiHangHoa int,
    @TenHangHoa [nvarchar](max),
    @TenHangHoa_KhongDau [nvarchar](max),
    @TenHangHoa_KyTuDau [nvarchar](max),
    @GhiChu [nvarchar](max),
    @QuyCach [nvarchar](max),
    @DuocBanTrucTiep [bit],
    @MaDonViCoBan [nvarchar](max),
    @MaHangHoa [nvarchar](max),
    @TenDonViTinh [nvarchar](max),
    @GiaVon [nvarchar](max),
    @GiaBan [nvarchar](max),
	@TonKho [nvarchar](max),
    @LaDonViChuan [bit],
    @TyLeChuyenDoi [nvarchar](max),  
    @MaHangHoaChaCungLoai [nvarchar](max),
	@DVTQuyCach nvarchar(max)
AS
BEGIN
		SET NOCOUNT ON;
		declare @dtNow datetime = getdate()
		declare @LaHangHoa bit ='true'

		if @LoaiHangHoa != 1
			set @LaHangHoa ='false'

		--declare @LoaiHangHoa int = iif(@LaHangHoa='true', 1,2)   

		DECLARE @ID_NhomHangHoaCha  uniqueidentifier = null;
    	if (len(@TenNhomHangHoaCha) > 0)
    	Begin		
			SELECT TOP(1) @ID_NhomHangHoaCha = ID FROM DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoaCha and LaNhomHangHoa = @LaHangHoa and (TrangThai is NULL or TrangThai = 0)
    		if (@ID_NhomHangHoaCha is null or len(@ID_NhomHangHoaCha) = 0)
    		Begin		
				set @ID_NhomHangHoaCha = NEWID()
    			insert into DM_NhomHangHoa (ID, TenNhomHangHoa,TenNhomHangHoa_KhongDau, TenNhomHangHoa_KyTuDau, MaNhomHangHoa, HienThi_BanThe, HienThi_Chinh, HienThi_Phu, LaNhomHangHoa, NguoiTao, NgayTao, ID_Parent, TrangThai)
    			values (@ID_NhomHangHoaCha, @TenNhomHangHoaCha,@TenNhomHangHoaCha_KhongDau, @TenNhomHangHoaCha_KyTuDau, @MaNhomHangHoaCha, '1', '1', '1', @LaHangHoa, 'admin', @dtNow, null, 0)
    		End
    	End
  	
		DECLARE @ID_NhomHangHoa  uniqueidentifier = null		
    	if (len(@TenNhomHangHoa) > 0)
    	Begin    	
			SELECT TOP(1) @ID_NhomHangHoa = ID FROM DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoa and LaNhomHangHoa = @LaHangHoa and (TrangThai is NULL or TrangThai = 0)
    		if (@ID_NhomHangHoa is null or len(@ID_NhomHangHoa) = 0)
    			Begin
					--- neu chuyen tu hanghoa --> dichvu (hoac nguoc lai)
					select TOP(1) @ID_NhomHangHoa = ID from DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoa  and (TrangThai is NULL or TrangThai = 0)
					if @ID_NhomHangHoa is null	
					begin
						set @ID_NhomHangHoa = NEWID()
    					insert into DM_NhomHangHoa (ID, TenNhomHangHoa,TenNhomHangHoa_KhongDau, TenNhomHangHoa_KyTuDau, MaNhomHangHoa, HienThi_BanThe, HienThi_Chinh, HienThi_Phu, LaNhomHangHoa, NguoiTao, NgayTao, ID_Parent, TrangThai)
    					values (@ID_NhomHangHoa, @TenNhomHangHoa, @TenNhomHangHoa_KhongDau, @TenNhomHangHoa_KyTuDau, @MaNhomHangHoa, '1', '1', '1', @LaHangHoa, 'admin', @dtNow, @ID_NhomHangHoaCha, 0)
					end
					
    			End
    	End
		else
			begin
				if @LaHangHoa='false' 
					set @ID_NhomHangHoa ='00000000-0000-0000-0000-000000000001'
				else 
					set @ID_NhomHangHoa ='00000000-0000-0000-0000-000000000000'
			end

		DECLARE @LaChaCungLoai  bit  = '1';
		DECLARE @TenCungLoai  nvarchar(max) = ''
    	DECLARE @ID_HangHoaCungLoai  uniqueidentifier  = newID();

    	if(len(@MaHangHoaChaCungLoai) > 0)
    	Begin    		
				select top 1 @ID_HangHoaCungLoai =ID_HangHoaCungLoai, 
							@TenCungLoai = TenHangHoa 
					from DM_HangHoa hh
					join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa
					where qd.MaHangHoa = @MaHangHoaChaCungLoai and LaChaCungLoai = '1';
    		if @ID_HangHoaCungLoai  is null   		
    			set @ID_HangHoaCungLoai = newID();
			else 
				begin
    				set @LaChaCungLoai = '0'; 
					if @TenHangHoa='' set @TenHangHoa = @TenCungLoai						
				end
    	End
		else
		begin
			if @isUpdateHang = 1 
				select top 1 @ID_HangHoaCungLoai = ID_HangHoaCungLoai from DM_HangHoa where ID = @ID_HangHoa
		end

		if @isUpdateHang = 1 
			begin
    			if (@MaDonViCoBan = '' or len (@MaDonViCoBan) = 0)
    			Begin
    				update DM_HangHoa set ID_HangHoaCungLoai = @ID_HangHoaCungLoai, LaChaCungLoai = @LaChaCungLoai, ID_NhomHang = @ID_NhomHangHoa,
							LaHangHoa = @LaHangHoa, LoaiHangHoa= @LoaiHangHoa,
    						NgaySua = @dtNow, NguoiSua='admin', 
								TenHangHoa = iif(@TenHangHoa='', TenHangHoa, @TenHangHoa), 
								TenHangHoa_KhongDau = iif(@TenHangHoa_KhongDau='', TenHangHoa_KhongDau, @TenHangHoa_KhongDau),   
								TenHangHoa_KyTuDau = iif(@TenHangHoa_KyTuDau='', TenHangHoa_KyTuDau, @TenHangHoa_KyTuDau),
    						TenKhac = @MaHangHoaChaCungLoai, GhiChu = @GhiChu, QuyCach = @QuyCach, DuocBanTrucTiep = @DuocBanTrucTiep, DonViTinhQuyCach= @DVTQuyCach 
					Where ID = @ID_HangHoa
    			end

    			update DonViQuiDoi set TenDonViTinh = @TenDonViTinh, TyLeChuyenDoi = @TyLeChuyenDoi, LaDonViChuan = @LaDonViChuan, GiaBan = @GiaBan, NguoiSua ='admin', NgaySua =@dtNow, GhiChu = @GhiChu
    			Where ID = @ID_DonViQuiDoi

			end
		else
			begin
					if (@MaDonViCoBan = '' or len (@MaDonViCoBan) = 0)
    				Begin
    					insert into DM_HangHoa (ID, ID_HangHoaCungLoai, LaChaCungLoai, ID_NhomHang, LaHangHoa, NgayTao,NguoiTao, TenHangHoa,TenHangHoa_KhongDau, TenHangHoa_KyTuDau,
    					TenKhac, TheoDoi, GhiChu, ChiPhiThucHien, ChiPhiTinhTheoPT, QuyCach, DuocBanTrucTiep, QuanLyTheoLoHang, DonViTinhQuyCach, LoaiHangHoa)
    					Values (@ID_HangHoa, @ID_HangHoaCungLoai, @LaChaCungLoai ,@ID_NhomHangHoa, @LaHangHoa, @dtNow, 'admin', @TenHangHoa, @TenHangHoa_KhongDau, @TenHangHoa_KyTuDau,
    					@MaHangHoaChaCungLoai, '1', @GhiChu, '0', '1', @QuyCach, @DuocBanTrucTiep, '0',@DVTQuyCach, @LoaiHangHoa)
    				end
    				else
    				Begin
						declare @ID_QDChuan uniqueidentifier 
						select TOP(1) @ID_HangHoa = ID_HangHoa, @ID_QDChuan= ID from DonViQuiDoi where MaHangHoa like @MaDonViCoBan						
    				end

    				insert into DonViQuiDoi (ID, MaHangHoa, TenDonViTinh, ID_HangHoa, TyLeChuyenDoi, LaDonViChuan, GiaVon, GiaNhap, GiaBan, NguoiTao, NgayTao, Xoa, GhiChu)
    				Values (@ID_DonViQuiDoi, @MaHangHoa,@TenDonViTinh, @ID_HangHoa,@TyLeChuyenDoi, @LaDonViChuan, @GiaVon, @GiaVon,@GiaBan, 'admin', @dtNow, '0', @GhiChu)
			end    
	DECLARE @FTonKho FLOAT;
	SET @FTonKho = CAST(@TonKho AS float);
	exec UpdateTonKho_multipleDVT @isUpdateTonKho, @ID_DonVi, @ID_DonViQuiDoi, null, @FTonKho
END");

            Sql(@"ALTER PROCEDURE [dbo].[import_DanhMucHangHoaLoHang]
	@isUpdateHang int,
	@isUpdateTonKho int,
	@ID_DonVi uniqueidentifier,
	@ID_HangHoa uniqueidentifier,
	@ID_DonViQuiDoi [uniqueidentifier],
	@ID_LoHang [uniqueidentifier] = null,
    @TenNhomHangHoaCha [nvarchar](max),
    @TenNhomHangHoaCha_KhongDau [nvarchar](max),
    @TenNhomHangHoaCha_KyTuDau [nvarchar](max),
    @MaNhomHangHoaCha [nvarchar](max),
    @TenNhomHangHoa [nvarchar](max),
    @TenNhomHangHoa_KhongDau [nvarchar](max),
    @TenNhomHangHoa_KyTuDau [nvarchar](max),
    @MaNhomHangHoa [nvarchar](max),
    @LoaiHangHoa int,
    @TenHangHoa [nvarchar](max),
    @TenHangHoa_KhongDau [nvarchar](max),
    @TenHangHoa_KyTuDau [nvarchar](max),
    @GhiChu [nvarchar](max),
    @QuyCach [nvarchar](max),
    @DuocBanTrucTiep [bit],
    @MaDonViCoBan [nvarchar](max),
    @MaHangHoa [nvarchar](max),
    @TenDonViTinh [nvarchar](max),
    @GiaVon [nvarchar](max),
    @GiaBan [nvarchar](max),
	@TonKho [nvarchar](max),
    @LaDonViChuan [bit],
    @TyLeChuyenDoi [nvarchar](max),
    @MaHangHoaChaCungLoai [nvarchar](max),
	@DVTQuyCach nvarchar(max),
    @MaLoHang [nvarchar](max),   
	@NgaySanXuat [datetime] =null,
    @NgayHetHan [datetime] = null  
AS
BEGIN
		SET NOCOUNT ON;
		declare @dtNow datetime = getdate()
		declare @LaHangHoa bit = 'true'
		declare @ID_QDChuan uniqueidentifier 
		if @LoaiHangHoa !=1
			set @LaHangHoa ='false'

		declare @QuanLyTheoLoHang bit = 'false'
		IF(@LaHangHoa = 'true' and @MaLoHang!='')  
			SET @QuanLyTheoLoHang = 'true'
  
		DECLARE @ID_NhomHangHoaCha  uniqueidentifier = null;   
    	if (len(@TenNhomHangHoaCha) > 0)
    	Begin    		
			select TOP(1) @ID_NhomHangHoaCha = ID from DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoaCha and LaNhomHangHoa = @LaHangHoa and (TrangThai is NULL or TrangThai = 0)
    		if (@ID_NhomHangHoaCha is null or len(@ID_NhomHangHoaCha) = 0)
    		BeGin
				set @ID_NhomHangHoaCha = NEWID()
    			insert into DM_NhomHangHoa (ID, TenNhomHangHoa, TenNhomHangHoa_KhongDau, TenNhomHangHoa_KyTuDau, MaNhomHangHoa, HienThi_BanThe, HienThi_Chinh, HienThi_Phu, LaNhomHangHoa, NguoiTao, NgayTao, ID_Parent, TrangThai)
    			values (@ID_NhomHangHoaCha, @TenNhomHangHoaCha, @TenNhomHangHoaCha_KhongDau, @TenNhomHangHoaCha_KyTuDau, @MaNhomHangHoaCha, '1', '1', '1', @LaHangHoa, 'admin', @dtNow, null, 0)
    		End
    	End

    	DECLARE @ID_NhomHangHoa  uniqueidentifier = null; 		
    	if (len(@TenNhomHangHoa) > 0)
    	Begin							
			select TOP(1) @ID_NhomHangHoa = ID from DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoa and LaNhomHangHoa = @LaHangHoa and (TrangThai is NULL or TrangThai = 0)
    		if @ID_NhomHangHoa is null 
    		BeGin    
				--- neu chuyen tu hanghoa --> dichvu (hoac nguoc lai)
				select TOP(1) @ID_NhomHangHoa = ID from DM_NhomHangHoa where TenNhomHangHoa like @TenNhomHangHoa  and (TrangThai is NULL or TrangThai = 0)
				if @ID_NhomHangHoa is null	
					begin
						set @ID_NhomHangHoa = NEWID()
    					insert into DM_NhomHangHoa (ID, TenNhomHangHoa, TenNhomHangHoa_KhongDau, TenNhomHangHoa_KyTuDau, MaNhomHangHoa, HienThi_BanThe, HienThi_Chinh, HienThi_Phu, LaNhomHangHoa, NguoiTao, NgayTao, ID_Parent, TrangThai)
    					values (@ID_NhomHangHoa, @TenNhomHangHoa, @TenNhomHangHoa_KhongDau, @TenNhomHangHoa_KyTuDau, @MaNhomHangHoa, '1', '1', '1', @LaHangHoa, 'admin', @dtNow, @ID_NhomHangHoaCha, 0)
					end
				--else
					--update DM_NhomHangHoa set LaNhomHangHoa= @LaHangHoa where ID = @ID_NhomHangHoa
    		End
    	End
		else
			begin
				if @LaHangHoa='false' 
					set @ID_NhomHangHoa ='00000000-0000-0000-0000-000000000001'
				else 
					set @ID_NhomHangHoa ='00000000-0000-0000-0000-000000000000'
			end
    
    	DECLARE @LaChaCungLoai  bit  = '1';
		DECLARE @TenCungLoai  nvarchar(max) = ''
    	DECLARE @ID_HangHoaCungLoai  uniqueidentifier  = newID();

    	if(len(@MaHangHoaChaCungLoai) > 0)
    	Begin 
    		select TOP(1) @ID_HangHoaCungLoai = ID_HangHoaCungLoai,
					@TenCungLoai = TenHangHoa  from DM_HangHoa hh
						join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa
						where qd.MaHangHoa = @MaHangHoaChaCungLoai and LaChaCungLoai = '1';
    		if @ID_HangHoaCungLoai  is null   		
    			set @ID_HangHoaCungLoai = newID();
			else 
				begin
    				set @LaChaCungLoai = '0'; 
					if @TenHangHoa='' set @TenHangHoa = @TenCungLoai	
				end
    	End
		begin
			if @isUpdateHang = 1 
				select top 1 @ID_HangHoaCungLoai = ID_HangHoaCungLoai from DM_HangHoa where ID = @ID_HangHoa
		end

		if @isUpdateHang = 1 
			Begin
				if (@MaDonViCoBan = '' or len (@MaDonViCoBan) = 0)
    				Begin
    					update DM_HangHoa set ID_HangHoaCungLoai = @ID_HangHoaCungLoai, LaChaCungLoai = @LaChaCungLoai, ID_NhomHang = @ID_NhomHangHoa, 
								LaHangHoa = @LaHangHoa, LoaiHangHoa= @LoaiHangHoa,
    							NgaySua = @dtNow, NguoiSua='admin', 
								TenHangHoa = iif(@TenHangHoa='', TenHangHoa, @TenHangHoa), 
								TenHangHoa_KhongDau = iif(@TenHangHoa_KhongDau='', TenHangHoa_KhongDau, @TenHangHoa_KhongDau),   
								TenHangHoa_KyTuDau = iif(@TenHangHoa_KyTuDau='', TenHangHoa_KyTuDau, @TenHangHoa_KyTuDau),
    							TenKhac = @MaHangHoaChaCungLoai, GhiChu = @GhiChu, QuyCach = @QuyCach, DuocBanTrucTiep = @DuocBanTrucTiep, DonViTinhQuyCach= @DVTQuyCach 
						Where ID = @ID_HangHoa
    				end				

    				update DonViQuiDoi set TenDonViTinh = @TenDonViTinh, TyLeChuyenDoi = @TyLeChuyenDoi, LaDonViChuan = @LaDonViChuan, GiaBan = @GiaBan, NguoiSua ='admin', NgaySua =@dtNow, GhiChu= @GhiChu
    				Where ID = @ID_DonViQuiDoi
						
					if(@QuanLyTheoLoHang = 'true')
					begin
						declare @countLo int = (select count(id) from DM_LoHang where id= @ID_LoHang)
						if @countLo = 0
							insert into DM_LoHang (ID, ID_HangHoa, MaLoHang, TenLoHang, NgaySanXuat, NgayHetHan, NguoiTao, NgayTao,TrangThai)
							values (@ID_LoHang, @ID_HangHoa, @MaLoHang, @MaLoHang, @NgaySanXuat, @NgayHetHan, 'admin',@dtNow,1)
						else
							if @NgaySanXuat is not null or @NgayHetHan is not null
								update DM_LoHang set NgaySanXuat = @NgaySanXuat, NgayHetHan = @NgayHetHan where ID = @ID_LoHang																					
					end													
			end
		else
		begin    		
    		if (@MaDonViCoBan = '' or len (@MaDonViCoBan) = 0)
    			Begin						
    				insert into DM_HangHoa (ID, ID_HangHoaCungLoai, LaChaCungLoai, ID_NhomHang, LaHangHoa, NgayTao,NguoiTao, TenHangHoa,TenHangHoa_KhongDau, TenHangHoa_KyTuDau,
    				TenKhac, TheoDoi, GhiChu, ChiPhiThucHien, ChiPhiTinhTheoPT, QuyCach, DuocBanTrucTiep, QuanLyTheoLoHang, DonViTinhQuyCach, LoaiHangHoa)
    				Values (@ID_HangHoa, @ID_HangHoaCungLoai, @LaChaCungLoai ,@ID_NhomHangHoa, @LaHangHoa, @dtNow, 'admin', @TenHangHoa, @TenHangHoa_KhongDau, @TenHangHoa_KyTuDau,
    				@MaHangHoaChaCungLoai, '1', @GhiChu, '0', '1', @QuyCach, @DuocBanTrucTiep, @QuanLyTheoLoHang, @DVTQuyCach, @LoaiHangHoa)
    
    				if(@MaLoHang != '' and @QuanLyTheoLoHang = 'true')
    				Begin    				    					
    					insert into DM_LoHang(ID, ID_HangHoa, MaLoHang, TenLoHang, NgaySanXuat, NgayHetHan, NguoiTao, NgayTao, TrangThai)
    					values (@ID_LoHang, @ID_HangHoa, @MaLoHang, @MaLoHang, @NgaySanXuat, @NgayHetHan, 'admin', GETDATE(), '1')
    				End
    			end
    		else
    			Begin								
					SELECT TOP(1) @ID_HangHoa = ID_HangHoa, @ID_QDChuan= ID from DonViQuiDoi where MaHangHoa like @MaDonViCoBan
					Select TOP(1) @ID_LoHang = ID FROM DM_LoHang where MaLoHang = @MaLoHang and ID_HangHoa = @ID_HangHoa									
    			end
				
    		insert into DonViQuiDoi (ID, MaHangHoa, TenDonViTinh, ID_HangHoa, TyLeChuyenDoi, LaDonViChuan, GiaVon, GiaNhap, GiaBan, NguoiTao, NgayTao, Xoa, GhiChu)
    		Values (@ID_DonViQuiDoi, @MaHangHoa,@TenDonViTinh, @ID_HangHoa,@TyLeChuyenDoi, @LaDonViChuan, @GiaVon, '0',@GiaBan, 'admin', @dtNow, '0', @GhiChu)			
   		end
	
	exec UpdateTonKho_multipleDVT @isUpdateTonKho, @ID_DonVi, @ID_DonViQuiDoi, @ID_LoHang, @TonKho
END");

            Sql(@"ALTER PROCEDURE [dbo].[Insert_LichNhacBaoDuong]
    @ID_HoaDon [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON	
    
    	declare @dtNow datetime = format(DATEADD(day,-30, getdate()),'yyyy-MM-dd')
    
    	declare @SoKmMacDinhNgay int= 30, @countSC int =0;
    	---- getthongtin hoadon	
    	declare @NgayLapHoaDon datetime , @ID_Xe uniqueidentifier, @ID_PhieuTiepNhan uniqueidentifier, @Now_SoKmVao float, @Now_NgayVaoXuong datetime
    	select @ID_PhieuTiepNhan = ID_PhieuTiepNhan, @NgayLapHoaDon = NgayLapHoaDon from BH_HoaDon where id= @ID_HoaDon
    
    		---- get thongtin tiepnhan hientai		
    	select @Now_SoKmVao = isnull(SoKmVao,0), @ID_Xe = ID_Xe, @Now_NgayVaoXuong = NgayVaoXuong
    	from Gara_PhieuTiepNhan
    	where ID = @ID_PhieuTiepNhan
    			
    
    	---- get thongtin tiepnhan gan nhat
    	declare @NgayXuatXuong_GanNhat datetime , @SoKmRa_GanNhat float
    	select top 1 @NgayXuatXuong_GanNhat = isnull(NgayXuatXuong, NgayVaoXuong) ,  @SoKmRa_GanNhat= ISNULL(SoKmRa,0) 
    	from Gara_PhieuTiepNhan where isnull(NgayXuatXuong, NgayVaoXuong) < @Now_NgayVaoXuong and ID_Xe= @ID_Xe 
    	order by NgayVaoXuong
    
    	if @NgayXuatXuong_GanNhat is not null
    		begin
    			set @SoKmMacDinhNgay =  CEILING( iif(@Now_SoKmVao - @SoKmRa_GanNhat=0,1, @Now_SoKmVao -@SoKmRa_GanNhat)/ iif(DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)=0,1,DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)))
    		end
    
    
    	----- get chitiet phutung thuoc hoadon có cài đặt bảo dưỡng
    	select distinct bd.LoaiBaoDuong,
    			bd.ID_HangHoa, 
    			bd.LanBaoDuong, 
    			iif(bd.BaoDuongLapDinhKy=1, bd.GiaTri,0) as GiaTriLap,
    			(select dbo.BaoDuong_GetTongGiaTriNhac(bd.LanBaoDuong,bd.ID_HangHoa)) as GiaTri,	
    			bd.LoaiGiaTri,
    			bd.BaoDuongLapDinhKy, bd.ID_LichBaoDuong, bd.GhiChu
    	into #tmpPhuTung
    	from 
    	(select hh.LoaiBaoDuong, qd.ID_HangHoa,  bd.LanBaoDuong, 
    		bd.GiaTri,	
    		bd.LoaiGiaTri,
    		bd.BaoDuongLapDinhKy,
    		max(ct.ID_LichBaoDuong) as ID_LichBaoDuong,
    		max(ct.GhiChu) as GhiChu
    		from BH_HoaDon_ChiTiet ct		
    		join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
    		join DM_HangHoa_BaoDuongChiTiet bd on hh.ID= bd.ID_HangHoa	
    		where ct.ID_HoaDon= @ID_HoaDon
    		and hh.QuanLyBaoDuong=1
    		and hh.LoaiBaoDuong !=0
    		and (ct.ID_ChiTietDinhLuong is null or ct.ID= ct.ID_ChiTietDinhLuong)
    		group by qd.ID_HangHoa,hh.LoaiBaoDuong, qd.ID_HangHoa,  bd.LanBaoDuong, 
    		bd.GiaTri,	
    		bd.LoaiGiaTri,
    		bd.BaoDuongLapDinhKy
    	) bd
    	order by bd.LanBaoDuong desc ---- nếu cùng 1 phụ tùng (vừa mua mới + bảo dưỡng) --> ưu tiên lấy phụ tùng bảo dưỡng	
    
    	---- get phụ tùng đã có lịch bảo dưỡng, và chưa dc xử lý
    		select lich.ID, lich.LanBaoDuong as LanBaoDuongThu, lich.SoKmBaoDuong, lich.NgayBaoDuongDuKien, pt.*
    		into #tmpLich
    		from #tmpPhuTung pt
    		join Gara_LichBaoDuong lich on lich.ID_Xe= @ID_Xe and lich.ID_HangHoa= pt.ID_HangHoa
    		where lich.TrangThai = 1 
    			
    		---- Nếu phụ tùng được thay mới ----> update lichcu trangthai =0) + insert lịch mới
    		update  lich set lich.TrangThai= 0
    		from Gara_LichBaoDuong lich
    		where exists (
    		select *
    		from #tmpLich tmp
    		where tmp.ID_LichBaoDuong is null and tmp.ID= lich.ID)
    		 
    			insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao, GhiChu, LanNhac)
    			select NEWID() as ID, a.ID_HangHoa,@ID_HoaDon, @ID_Xe, a.LanBaoDuong,
    				a.SoKmBaoDuong, a.NgayBaoDuongDuKien, a.TrangThai, a.NgayTao, a.GhiChu, 0
    			from
    			(
    				select pt.ID_HangHoa, pt.LanBaoDuong,
    				case pt.LoaiBaoDuong
    					when 2 then @Now_SoKmVao + pt.GiaTri --- chi lưu cột này nếu loại bảo dưỡng = KM
    					else 0 end as SoKmBaoDuong,
    				case pt.LoaiBaoDuong
    					when 2 then DATEADD(day, CEILING( pt.GiaTri/@SoKmMacDinhNgay), @NgayLapHoaDon) --- get số ngày theo km mặc định + thời gian tiếp nhận
    					when 1 then DATEADD(day, pt.GiaTri, @NgayLapHoaDon)	
    				end as NgayBaoDuongDuKien,
    				1 as TrangThai,
    				GETDATE() as NgayTao,
    				pt.GhiChu
    				from
    				(
    					select distinct  ID_HangHoa, LanBaoDuong, LoaiBaoDuong, GiaTri, LoaiGiaTri, GhiChu
    					from #tmpLich 
    					where ID_LichBaoDuong is null --- phụ tùng có lịch bảo dưỡng, nhưng thay mới
    				) pt
    			) a where a.NgayBaoDuongDuKien >= @dtNow
    		
    			---- Nếu phụ tùng đi bảo dưỡng, nhưng ngày bảo dưỡng (hiện tại) gần sát với ngày nhắc dự kiến lần tiếp theo --> xóa và thêm mới
    			---- (gần sát ở đây dc mặc định là < 1/2 thời gian)
    			select *
    			into #lichSatNgay
    			from
    			(
    				select lichOld.ID, lichOld.LanBaoDuongThu, lichOld.ID_HangHoa,
    					 lichOld.LanBaoDuong, lichOld.LoaiBaoDuong, lichOld.GiaTri, lichOld.LoaiGiaTri,
    					lichOld.GiaTri as GiaTriMin,
    					lichOld.SoKmBaoDuong, @Now_SoKmVao as sokmvaomow, GhiChu,
    					case lichOld.LoaiGiaTri
    						when 4 then lichOld.SoKmBaoDuong - @Now_SoKmVao
    						else
    						DATEDIFF(day,@NgayLapHoaDon, lichOld.NgayBaoDuongDuKien) end as GiaTriLech --- chênh lệch giữa ngày bảo dưỡng hiện tại và ngày dự kiến
    				from #tmpLich lichOld
    				where ID_LichBaoDuong is not null -- phụ tùng đi bảo dưỡng
    				and (lichOld.LanBaoDuong = lichOld.LanBaoDuongThu + 1)				
    			) b where b.GiaTriLech >  b.GiaTriMin/2 --- neu tiepnhanxe voi sokmvao = 0, giatrilech bi am			
    			
    			
    		  ---- Xoa va insert lai
    			delete  lich			
    			from Gara_LichBaoDuong lich
    			where exists (
    			select tmp.ID from #lichSatNgay tmp where  tmp.ID= lich.ID)
    			
    			
    			insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao, GhiChu, LanNhac)
    			select NEWID() as ID, a.ID_HangHoa,@ID_HoaDon, @ID_Xe, a.LanBaoDuong,
    				a.SoKmBaoDuong, a.NgayBaoDuongDuKien, a.TrangThai, a.NgayTao, a.GhiChu,0
    				from
    				(
    					select pt.ID_HangHoa, pt.LanBaoDuong, pt.GhiChu,
    					case pt.LoaiBaoDuong
    						when 2 then @Now_SoKmVao + pt.GiaTri --- chi lưu cột này nếu loại bảo dưỡng = KM
    						else 0 end as SoKmBaoDuong,
    					case pt.LoaiBaoDuong
    						when 2 then DATEADD(day, CEILING( pt.GiaTri/@SoKmMacDinhNgay), @NgayLapHoaDon)
    						when 1 then DATEADD(day, pt.GiaTri, @NgayLapHoaDon)										
    					end as NgayBaoDuongDuKien,
    					1 as TrangThai,
    					GETDATE() as NgayTao
    					from 
    					(
    						select distinct  ID_HangHoa, LanBaoDuong, LoaiBaoDuong, GiaTri, LoaiGiaTri, GhiChu
    						from #lichSatNgay 			
    					) pt
    				
    			) a where a.NgayBaoDuongDuKien >= @dtNow
    			
    
    			----- insert phutung nếu chưa có trong lịch bảo dưỡng 
    			insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao, GhiChu, LanNhac)
    			select NEWID() as ID, a.ID_HangHoa, @ID_HoaDon, @ID_Xe, a.LanBaoDuong,
    				a.SoKmBaoDuong, a.NgayBaoDuongDuKien, a.TrangThai, a.NgayTao, a.GhiChu,0
    			from
    			(
    				select  pt.ID_HangHoa, pt.LanBaoDuong, pt.GhiChu,
    				case pt.LoaiBaoDuong
    					when 2 then @Now_SoKmVao + pt.GiaTri --- chi lưu cột này nếu loại bảo dưỡng = KM
    					else 0 end as SoKmBaoDuong,
    				case pt.LoaiBaoDuong
    					when 2 then DATEADD(day, CEILING( pt.GiaTri/@SoKmMacDinhNgay), @NgayLapHoaDon)
    					when 1 then DATEADD(day, pt.GiaTri, @NgayLapHoaDon)									
    				end as NgayBaoDuongDuKien,
    				1 as TrangThai,
    				GETDATE() as NgayTao
    				from #tmpPhuTung pt
    				where not exists (
    					select lich.ID from Gara_LichBaoDuong lich
    					where lich.ID_HangHoa= pt.ID_HangHoa and lich.ID_Xe= @ID_Xe 
						and (
							(pt.BaoDuongLapDinhKy =1 and lich.TrangThai !=0)
							or (pt.BaoDuongLapDinhKy =0 )
							)
						
    				) 
    			) a where a.NgayBaoDuongDuKien >= @dtNow --- chi insert neu lich > ngayhientai
    
    			---- insert phutung da colich, nhung cai dat lap lai
    			insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao, GhiChu, LanNhac)
    			select NEWID() as ID, a.ID_HangHoa, @ID_HoaDon,  @ID_Xe, a.LanBaoDuongThu,
    				a.SoKmBaoDuong, a.NgayBaoDuongDuKien, a.TrangThai, a.NgayTao, a.GhiChu,0
    			from
    			(
    				select  pt.ID_HangHoa, pt.LanBaoDuongThu, pt.GhiChu,
    					case pt.LoaiBaoDuong
    						when 2 then @Now_SoKmVao + pt.GiaTri --- lay sokmbaoduong o lancuoicung (cua lichbaoduong) + giatri
    						else 0 end as SoKmBaoDuong,
    					case pt.LoaiBaoDuong
    						when 2 then DATEADD(day, CEILING( pt.GiaTri/@SoKmMacDinhNgay), @NgayLapHoaDon) --- get số ngày theo km mặc định + thời gian tiếp nhận
    						when 1 then DATEADD(day, pt.GiaTri, @NgayLapHoaDon)									
    					end as NgayBaoDuongDuKien,
    					1 as TrangThai,
    					GETDATE() as NgayTao
    				from
    					(----- get phutung da co lichbaoduong, va caidat laplai	
    					----- chi insert neu lanbaoduong cuoicung da duoc xuly baoduong (trangthai = 2)
    					select *
    					from (
    						select lich.ID, lich.TrangThai, lich.LanBaoDuong + 1 as LanBaoDuongThu, lich.SoKmBaoDuong, lich.NgayBaoDuongDuKien, pt.*,
    						ROW_NUMBER() over(partition by lich.ID_HangHoa order by lich.LanBaoDuong desc) as RN
    						from #tmpPhuTung pt
    						join Gara_LichBaoDuong lich on lich.ID_Xe= @ID_Xe and lich.ID_HangHoa= pt.ID_HangHoa
    						where pt.BaoDuongLapDinhKy= 1 and lich.TrangThai !=0 ---- khong get lich bi xoa				
    					) b where b.RN= 1 and b.TrangThai = 2			
    				) pt  where pt.RN= 1
    			) a where a.NgayBaoDuongDuKien >= @dtNow --- chi insert neu lich > ngayhientai
END");

            Sql(@"ALTER PROCEDURE [dbo].[insertChotSo_XuatNhapTon]
    @NgayChotSo [datetime],
    @ID_ChiNhanh [uniqueidentifier]
AS
BEGIN
	set nocount on;
	declare @ToDate datetime = dateadd(day,1,@NgayChotSo)
	
	declare @ngayChotOld datetime = (select top 1 NgayChotSo from ChotSo_HangHoa)
	if @ngayChotOld is null or @NgayChotSo < @ngayChotOld --- chot so lui ngay
	begin
		--- chua chot so lan nao	
		set @ngayChotOld = '2016-01-01'
	end	
	
	---- get tonkho Từ đầu - thời gian chốt sổ
	 select 
				tkTrongKy.ID_HangHoa,
				tkTrongKy.ID_LoHang,
				tkTrongKy.SoLuongNhap - tkTrongKy.SoLuongXuat as SoLuongTon,
				tkTrongKy.GiaTriNhap - tkTrongKy.GiaTriXuat as GiaTriTon	
			into #tblTrongKy
			 from
			 (
					select 			
						qd.ID_HangHoa,
						tkNhapXuat.ID_LoHang,
						tkNhapXuat.ID_DonVi,				
						sum(tkNhapXuat.SoLuongNhap * qd.TyLeChuyenDoi) as SoLuongNhap,
						sum(tkNhapXuat.GiaTriNhap ) as GiaTriNhap,
						sum(tkNhapXuat.SoLuongXuat * qd.TyLeChuyenDoi) as SoLuongXuat,
						sum(tkNhapXuat.GiaTriXuat) as GiaTriXuat
					from
					(
					-- xuat ban, trahang ncc, xuatkho, xuat chuyenhang
						select 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							0 AS SoLuongNhap,
							0 AS GiaTriNhap,
							sum(
								case hd.LoaiHoaDon
								when 10 then ct.TienChietKhau
								else ct.SoLuong end ) as SoLuongXuat,
							sum( 
								case hd.LoaiHoaDon
								when 7 then ct.SoLuong* ct.DonGia
								when 10 then ct.TienChietKhau * ct.GiaVon
								else ct.SoLuong* ct.GiaVon end )  AS GiaTriXuat
							FROM BH_HoaDon_ChiTiet ct
						LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						WHERE hd.ChoThanhToan = 0
						and (hd.LoaiHoaDon in (1,5,7,8) 
							or (hd.LoaiHoaDon = 10  and (hd.YeuCau='1' or hd.YeuCau='4')) )
						AND hd.NgayLapHoaDon between  @ngayChotOld AND  @ToDate
						and hd.ID_DonVi= @ID_ChiNhanh
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi								


						UNION ALL
						 ---nhap chuyenhang
						SELECT 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_CheckIn AS ID_DonVi,
							SUM(ct.TienChietKhau) AS SoLuongNhap,
							SUM(ct.TienChietKhau* ct.DonGia) AS GiaTriNhap, -- lay giatri tu chinhanh chuyen
							0 AS SoLuongXuat,
							0 AS GiaTriXuat
						FROM BH_HoaDon_ChiTiet ct
						LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						WHERE hd.LoaiHoaDon = 10 and hd.YeuCau = '4' AND hd.ChoThanhToan = 0
						and hd.ID_DonVi= @ID_ChiNhanh
						AND hd.NgaySua between  @ngayChotOld AND @ToDate
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn

    					UNION ALL
						 ---nhaphang + khach trahang
						SELECT 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							SUM(ct.SoLuong) AS SoLuongNhap,
							--- KH trahang: giatrinhap = giavon (khong lay giaban)
							sum(iif(hd.LoaiHoaDon= 6, iif(ctm.GiaVon is null or ctm.ID = ctm.ID_ChiTietDinhLuong, ct.SoLuong * ct.GiaVon, ct.SoLuong *ctm.GiaVon),
							iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang))))  AS GiaTriNhap,
							0 AS SoLuongXuat,
							0 AS GiaTriXuat
						FROM BH_HoaDon_ChiTiet ct
						LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						left join BH_HoaDon_ChiTiet ctm on ct.ID_ChiTietGoiDV = ctm.ID
						WHERE (hd.LoaiHoaDon = '4' or hd.LoaiHoaDon = '6') 
						AND hd.ChoThanhToan = 0
						and hd.ID_DonVi= @ID_ChiNhanh
						AND hd.NgayLapHoaDon between  @ngayChotOld AND @ToDate
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi
    
    					UNION ALL
						-- kiemke
    					SELECT 
							ctkk.ID_DonViQuiDoi, 
							ctkk.ID_LoHang, 
							ctkk.ID_DonVi, 
							sum(isnull(SoLuongNhap,0)) as SoLuongNhap,
							sum(isnull(SoLuongNhap,0) * ctkk.GiaVon) as GiaTriNhap,
							sum(isnull(SoLuongXuat,0)) as SoLuongXuat,
							sum(isnull(SoLuongXuat,0) * ctkk.GiaVon) as GiaTriXuat
						FROM
    					(SELECT 
    						ct.ID_DonViQuiDoi,
    						ct.ID_LoHang,
							hd.ID_DonVi,
							IIF(ct.SoLuong< 0, 0, ct.SoLuong) as SoLuongNhap,
							IIF(ct.SoLuong < 0, - ct.SoLuong, 0) as SoLuongXuat,
							ct.GiaVon
    					FROM BH_HoaDon_ChiTiet ct 
    					LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
    					WHERE hd.LoaiHoaDon = '9' 
    					AND hd.ChoThanhToan = 0
						and hd.ID_DonVi= @ID_ChiNhanh
    					AND hd.NgayLapHoaDon between  @ngayChotOld AND @ToDate	
						) ctkk	
    					GROUP BY ctkk.ID_DonViQuiDoi, ctkk.ID_LoHang, ctkk.ID_DonVi
					)tkNhapXuat
					join DonViQuiDoi qd on tkNhapXuat.ID_DonViQuiDoi = qd.ID 
					group by qd.ID_HangHoa,tkNhapXuat.ID_LoHang, tkNhapXuat.ID_DonVi
				) tkTrongKy			
			
			
		---- check hàng chốt sổ bị sai
		select cs.*, tk.SoLuongTon as TonKhoDung, tk.GiaTriTon as GtriTonDung
		into #tblChotSoSai
		from ChotSo_HangHoa cs
		left join #tblTrongKy tk on cs.ID_HangHoa = tk.ID_HangHoa and (cs.ID_LoHang= tk.ID_LoHang or cs.ID_LoHang is null and tk.ID_LoHang is null)
		where cs.ID_DonVi= @ID_ChiNhanh
		and (cs.TonKho != tk.SoLuongTon )--or cs.GiaTriTon != tk.GiaTriTon)


		----- xóa hàng chốt sai && insert again
		delete cs		
		from ChotSo_HangHoa cs
		where exists (
		select ID from #tblChotSoSai tblSai where cs.ID= tblSai.ID
		)

		---- insert again hàng sai
		insert into ChotSo_HangHoa (ID, ID_DonVi, ID_HangHoa, ID_LoHang, NgayChotSo, TonKho) --, GiaTriTon) --- todo giatri ton
		select newID(),
			@ID_ChiNhanh,			
			ID_HangHoa,
			ID_LoHang,
			@NgayChotSo,
			ISNULL(tk.TonKhoDung,0) as TonKho --,
			--ISNULL(tk.GtriTonDung,0)  as GiaTriTon
		from #tblChotSoSai tk
	

		---- insert hàng chưa chốt sổ 
		insert into ChotSo_HangHoa (ID, ID_DonVi, ID_HangHoa, ID_LoHang, NgayChotSo, TonKho) --, GiaTriTon) --- todo giatri ton
		select newID(),
			@ID_ChiNhanh,			
			ID_HangHoa,
			ID_LoHang,
			@NgayChotSo,
			ISNULL(tk.SoLuongTon,0) as TonKho --,
			--ISNULL(tk.GtriTonDung,0)  as GiaTriTon
		from #tblTrongKy tk
	
END");

            Sql(@"ALTER PROCEDURE [dbo].[InsertLichNhacBaoDuong_whenQuaHan_orEnoughLanNhac]
    @ID_LichBaoDuong [uniqueidentifier],
    @TypeUpdate [int]
AS
BEGIN
    SET NOCOUNT ON;
    	declare @maxLanBaoDuong int, @max_soKmBaoDuong int, @now_LanBaoDuong int, @now_NgayBaoDuongDuKien datetime, @now_LanNhac int,
    	@ID_Xe uniqueidentifier, @ID_HoaDon uniqueidentifier, @ID_HangHoa uniqueidentifier
    	--- get infor lich
    	select  @ID_Xe = ID_Xe, @ID_HangHoa= ID_HangHoa, @ID_HoaDon= ID_HoaDon,
    		@now_NgayBaoDuongDuKien = NgayBaoDuongDuKien,
    		@now_LanBaoDuong = LanBaoDuong,
    		@now_LanNhac = LanNhac
    	from Gara_LichBaoDuong
    	where ID= @ID_LichBaoDuong
    	
    	--- get max lanbaoduong
    	select top 1 @maxLanBaoDuong = LanBaoDuong, @max_soKmBaoDuong= SoKmBaoDuong
    	from Gara_LichBaoDuong where ID_Xe= @ID_Xe and ID_HangHoa = @ID_HangHoa and TrangThai !=0 order by LanBaoDuong desc
    
    	
    	---- getcaidat lich
    	select *,
    		iif(LoaiGiaTri < 4, 1, 2) as LoaiBaoDuong --- 1.thoigian, 2.km
    	into #tblSetup
    	from DM_HangHoa_BaoDuongChiTiet where ID_HangHoa= @ID_HangHoa and BaoDuongLapDinhKy = 1
    
    	declare @countRepeater int = (select count(id) from #tblSetup)
    
    	select * from #tblSetup
    
    	
    
    	--- chi insert neu max lanbaoduong va laplai
    	if @now_LanBaoDuong = @maxLanBaoDuong and @countRepeater > 0
    	begin
    		
    		---- getthongtin tiepnhan theohoadon		
    		declare @SoKmMacDinhNgay int= 30, @countSC int =0;
    		declare  @ID_PhieuTiepNhan uniqueidentifier, @Now_SoKmVao float, @Now_NgayVaoXuong datetime
    		select @ID_PhieuTiepNhan = ID_PhieuTiepNhan from BH_HoaDon where id= @ID_HoaDon
    
    			---- get thongtin tiepnhan hientai		
    		select @Now_SoKmVao = isnull(SoKmVao,0), @ID_Xe = ID_Xe, @Now_NgayVaoXuong = NgayVaoXuong
    		from Gara_PhieuTiepNhan
    		where ID = @ID_PhieuTiepNhan
    
    		---- get thongtin tiepnhan gan nhat
    		declare @NgayXuatXuong_GanNhat datetime , @SoKmRa_GanNhat float
    		select top 1 @NgayXuatXuong_GanNhat = isnull(NgayXuatXuong, NgayVaoXuong) ,  @SoKmRa_GanNhat= ISNULL(SoKmRa,0) 
    		from Gara_PhieuTiepNhan where isnull(NgayXuatXuong, NgayVaoXuong) < @Now_NgayVaoXuong and ID_Xe= @ID_Xe 
    		order by NgayVaoXuong 
    
    	if @NgayXuatXuong_GanNhat is not null
    		begin
    			set @SoKmMacDinhNgay =  CEILING( iif(@Now_SoKmVao - @SoKmRa_GanNhat=0,1, @Now_SoKmVao -@SoKmRa_GanNhat)/ iif(DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)=0,1,DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)))
    		end
    
    	--	select @now_LanBaoDuong as now_LanBaoDuong,@maxLanBaoDuong as maxLanBaoDuong ,
    	--@max_soKmBaoDuong as max_soKmBaoDuong, @countRepeater as countRepeater,
    	--@now_NgayBaoDuongDuKien as now_NgayBaoDuongDuKien,
    	--@SoKmMacDinhNgay as SoKmMacDinhNgay,
    	--@Now_SoKmVao as kmvao,
    	-- @SoKmRa_GanNhat as kmra,
    	-- @NgayXuatXuong_GanNhat as ngayxuat,
    	-- @Now_NgayVaoXuong as ngayvao
    			
    		if @TypeUpdate= 1 --- quahan		
    			goto InsertLichNhac						  
    		
    		if @TypeUpdate= 2 --- đủ số lần nhắc
    		begin
    			declare @countSetup int , @SoLanNhac int
    			select @countSetup = COUNT(ID) OVER (), @SoLanNhac = SoLanLapLai from HT_ThongBao_CatDatThoiGian where LoaiThongBao= 4
    			select @countSetup , @SoLanNhac
    			if @countSetup > 0 and @SoLanNhac = @now_LanNhac
    				goto InsertLichNhac
    		end
    		
    		InsertLichNhac:
    			insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao, GhiChu)
    			select newid(), @ID_HangHoa, @ID_HoaDon, @ID_Xe, @maxLanBaoDuong + 1, SoKmBaoDuong, NgayBaoDuongDuKien, 1, GETDATE(),''
    			from(
    			select 
    					case pt.LoaiBaoDuong
    						when 2 then @max_soKmBaoDuong + pt.GiaTri
    						else 0 end as SoKmBaoDuong,
    					case pt.LoaiBaoDuong
    						when 2 then DATEADD(day, CEILING(pt.GiaTri/@SoKmMacDinhNgay), @now_NgayBaoDuongDuKien)																			
    						when 1 then DATEADD(day, pt.GiaTri, @now_NgayBaoDuongDuKien)										
    					end as NgayBaoDuongDuKien				
    				from #tblSetup pt
    				) pt			
    	end  
    	drop table #tblSetup
END");

            Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinh_ChiPhiSuaChua]
    @Year [int],
    @IdChiNhanh [uniqueidentifier],
    @LoaiBaoCao [int]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert statements for procedure here
    	IF(@LoaiBaoCao = 1)
    	BEGIN
    		SELECT Thang, SUM(ThanhTien) AS ChiPhi FROM
    		(SELECT MONTH(hd.NgayLapHoaDon) AS Thang, hdcp.ThanhTien FROM BH_HoaDon hd
    		INNER JOIN BH_HoaDon_ChiPhi hdcp ON hd.ID = hdcp.ID_HoaDon
    		WHERE YEAR(hd.NgayLapHoaDon) = @Year AND hd.ID_DonVi = @IdChiNhanh AND hd.ChoThanhToan = 0) AS a
    		GROUP BY Thang
    	END
    	ELSE IF(@LoaiBaoCao = 2)
    	BEGIN
    		SELECT Thang, SUM(ThanhTien) AS ChiPhi FROM
    		(SELECT YEAR(hd.NgayLapHoaDon) AS Thang, hdcp.ThanhTien FROM BH_HoaDon hd
    		INNER JOIN BH_HoaDon_ChiPhi hdcp ON hd.ID = hdcp.ID_HoaDon
    		WHERE YEAR(hd.NgayLapHoaDon) = @Year AND hd.ID_DonVi = @IdChiNhanh AND hd.ChoThanhToan = 0) AS a
    		GROUP BY Thang
    	END
END");

            Sql(@"ALTER PROCEDURE [dbo].[SaoChepThietLapLuong]
    @ID_ChiNhanh [uniqueidentifier],
    @ID_NhanVien [uniqueidentifier],
    @KieuLuongs [nvarchar](50),
    @IDNhanViens [nvarchar](max),
    @UpdateNVSetup [bit],
    @ID_NhanVienLogin [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @tblKieuLuong table(LoaiLuong int)
    	insert into @tblKieuLuong
    	select Name from dbo.splitstring(@KieuLuongs)
    
    	---- get tlapluong of nhanvien
    	select *
    	into #tempCoBan
    	from NS_Luong_PhuCap pc
    	where pc.ID_NhanVien = @ID_NhanVien and pc.ID_DonVi= @ID_ChiNhanh and pc.TrangThai !=0
    	and exists(select LoaiLuong from @tblKieuLuong loai where pc.LoaiLuong= loai.LoaiLuong)
    
    	---- get tlapluong chi tiet of nhanvien
    	select ct.*
    	into #tempChiTiet
    	from NS_Luong_PhuCap pc
    	join NS_ThietLapLuongChiTiet ct on pc.ID= ct.ID_LuongPhuCap
    	where pc.ID_NhanVien = @ID_NhanVien and pc.ID_DonVi= @ID_ChiNhanh
    	and pc.TrangThai !=0
    	and exists(select LoaiLuong from @tblKieuLuong loai where pc.LoaiLuong= loai.LoaiLuong)
    
    	declare @tblNhanVien table(ID_NhanVien uniqueidentifier)
    	if @UpdateNVSetup = '0'	
    		---- giu nguyen tlapluong cu (chi insert nhung nvien not exist in tlapluong)
    		insert into @tblNhanVien
    		select Name from dbo.splitstring(@IDNhanViens) tbl
   -- 		where not exists (select ID_NhanVien from NS_Luong_PhuCap pc 
			--where tbl.Name= pc.ID_NhanVien and pc.ID_DonVi= @ID_ChiNhanh and pc.TrangThai !=0)

			---- chi insert cac thietlapluong chua dc cai dat

    		
    	else
    		---- capnhat lai thietlapluong
    		begin
    			insert into @tblNhanVien
    			select Name from dbo.splitstring(@IDNhanViens)	where Name !=@ID_NhanVien
    
    			---- xoa thietlapluong exist (chỉ xóa những loại sao chép)
    			delete from NS_ThietLapLuongChiTiet 
    			where ID_LuongPhuCap in
    				(select ID 
    				from NS_Luong_PhuCap pc 
    				where pc.ID_DonVi = @ID_ChiNhanh
    				and exists( select ID_NhanVien from @tblNhanVien nv where pc.ID_NhanVien =nv.ID_NhanVien)
    				and exists(select LoaiLuong from @tblKieuLuong loai where pc.LoaiLuong= loai.LoaiLuong)
    				)
    
    			delete from NS_Luong_PhuCap
    			where ID_DonVi = @ID_ChiNhanh
    			and ID_NhanVien in ( select ID_NhanVien from @tblNhanVien)
    			and LoaiLuong in (select LoaiLuong from @tblKieuLuong)
    		end
    
    	declare @IDNhanVien uniqueidentifier
    	DECLARE curNhanVien CURSOR SCROLL LOCAL FOR
    		select ID_NhanVien
    		from @tblNhanVien 
    	OPEN curNhanVien 
    	FETCH FIRST FROM curNhanVien
    	INTO @IDNhanVien
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
    				---- insert tlapcoban (neu 1Nvien co nhieu thietlapluong coban)
    				declare @curIDPhuCap uniqueidentifier
    				DECLARE curPhuCapCB CURSOR SCROLL LOCAL FOR
    				select ID
    				from #tempCoBan 
    				OPEN curPhuCapCB 
    			FETCH FIRST FROM curPhuCapCB
    				INTO @curIDPhuCap
    				WHILE @@FETCH_STATUS = 0
    				begin					
    					declare @ID_PhuCap uniqueidentifier = NEWID()
    					--select  @ID_PhuCap
    					insert into NS_Luong_PhuCap
    					select @ID_PhuCap, @IDNhanVien, ID_LoaiLuong, NgayApDung, NgayKetThuc, SoTien, HeSo, Bac, NoiDung, TrangThai, LoaiLuong, ID_DonVi
    					from #tempCoBan where LoaiLuong not in (51,52,53,61,62,63) and ID= @curIDPhuCap
    
    					---- insert tlapnangcao of luong
    					insert into NS_ThietLapLuongChiTiet (ID, ID_LuongPhuCap, LuongNgayThuong, NgayThuong_LaPhanTramLuong, Thu7_GiaTri, Thu7_LaPhanTramLuong,
    						ThCN_GiaTri, CN_LaPhanTramLuong, NgayNghi_GiaTri, NgayNghi_LaPhanTramLuong, NgayLe_GiaTri, NgayLe_LaPhanTramLuong,
    						LaOT, ID_CaLamViec)
    					select NEWID(), @ID_PhuCap,LuongNgayThuong,NgayThuong_LaPhanTramLuong,Thu7_GiaTri,Thu7_LaPhanTramLuong,
    						ThCN_GiaTri, CN_LaPhanTramLuong, NgayNghi_GiaTri, NgayNghi_LaPhanTramLuong, NgayLe_GiaTri, NgayLe_LaPhanTramLuong,
    						LaOT, ID_CaLamViec
    					from #tempChiTiet where ID_LuongPhuCap= @curIDPhuCap
    
    					FETCH NEXT FROM curPhuCapCB 
    					INTO @curIDPhuCap
    				end
    				CLOSE curPhuCapCB  
    			DEALLOCATE curPhuCapCB 
    
    				-- insert phucap + giamtru
    				insert into NS_Luong_PhuCap
    				select NewID(), @IDNhanVien, ID_LoaiLuong, NgayApDung, NgayKetThuc, SoTien, HeSo, Bac, NoiDung, TrangThai, LoaiLuong, ID_DonVi
    				from #tempCoBan where LoaiLuong in (51,52,53,61,62,63)			
    
    				FETCH NEXT FROM curNhanVien 
    				INTO @IDNhanVien
    			END;
    			CLOSE curNhanVien  
    		DEALLOCATE curNhanVien 
    
    		declare @loailuong nvarchar(200) =''
    		if (select count(*) from @tblKieuLuong where LoaiLuong in(1,2,3,4)) > 0
    			set @loailuong =N'lương,'
    		if (select count(*) from @tblKieuLuong where LoaiLuong like '%5%') > 0
    			set @loailuong = @loailuong + N' phụ cấp,'
    		if (select count(*) from @tblKieuLuong where LoaiLuong like '%6%') > 0
    			set @loailuong = @loailuong + N' giảm trừ'
    
    		declare @tenNhanVien nvarchar(200), @maNhanVien nvarchar(100)
    		select @tenNhanVien = TenNhanVien, @maNhanVien = MaNhanVien from NS_NhanVien where ID= @ID_NhanVien
    
    		declare @nhanvienSetup nvarchar(max) = (
    		select  concat( TenNhanVien ,' (',MaNhanVien, ')') + ', ' as [text()] 
    		from NS_NhanVien nv1 
    		where exists (select ID from @tblNhanVien nv2 where nv1.ID= nv2.ID_NhanVien)
    		for xml path(''))
    		
    		insert into HT_NhatKySuDung (ID, ID_DonVi, ID_NhanVien, LoaiNhatKy, ChucNang, ThoiGian, NoiDung, NoiDungChiTiet)
    		values (NEWID(), @ID_ChiNhanh, @ID_NhanVienLogin, 1, N'Thiết lập lương - Sao chép', GETDATE(),
    		concat(N'Sao chép thiết lập lương ', N'(', @loailuong , N') từ nhân viên <b>', @tenNhanVien , ' (',  @maNhanVien ,' </b>)'),
    		concat(N'Sao chép thiết lập lương ', N'(', @loailuong , N') từ nhân viên <b>', @tenNhanVien , ' (',  @maNhanVien , N'</b>) đến: ', @nhanvienSetup) )
END");

            Sql(@"ALTER PROCEDURE [dbo].[SP_GetAllHoaDon_byIDPhieuThuChi]
    @ID_PhieuThuChi [varchar](50)
AS
BEGIN
	SET NOCOUNT ON;
	-- get list HD with MaPhieuThuChi
    select
		case when qhd.PhieuDieuChinhCongNo='1' then N'Điều chỉnh công nợ' else
			case when sum(qct.TienThu)= 0 and max(qct.DiemThanhToan) > 0 then N'Điều chỉnh điểm' else
					ISNULL(hd.MaHoaDon,N'Thu thêm') end end as MaHoaDon,
			ISNULL(hd.NgayLapHoaDon, max(qhd.NgayLapHoaDon)) as NgayLapHoaDon, 
			case when sum(qct.TienThu)= 0 then max(qct.DiemThanhToan) 
			else case when hd.ID_BaoHiem = max(qct.ID_DoiTuong) then ISNULL(hd.PhaiThanhToanBaoHiem,0)
				else ISNULL(hd.PhaiThanhToan,0) end  end as TongTienThu,
    		sum(qct.TienThu) as TienThu, 
			0 as DaChi,
    		N'Đã thanh toán' as GhiChu,
			max(qhd.NgayLapHoaDon) as NgayLapPhieu,
			max(isnull(qct.ID_DoiTuong,'00000000-0000-0000-0000-000000000000')) as ID_DoiTuong,
			max(isnull(qct.ID_KhoanThuChi,'00000000-0000-0000-0000-000000000000')) as ID_KhoanThuChi,
    		MAX(ISNULL(QuyXML.PhuongThucTT,N'Tiền mặt')) as PhuongThuc
		INTO #temp
    	from Quy_HoaDon_ChiTiet qct
		join Quy_HoaDon qhd on qhd.ID = qct.ID_HoaDon
    	left join BH_HoaDon hd  on qct.ID_HoaDonLienQuan = hd.ID -- truong hop thu them (left join)
    	LEFT JOIN 
    	(	
    				Select Main.ID_HoaDonLienQuan,
    				   Left(Main.PThuc_SoQuy,Len(Main.PThuc_SoQuy)-1) As PhuongThucTT
    				From
    				(
    					Select distinct main1.ID_HoaDonLienQuan, 
    						(
    							Select distinct (tbl1.PhuongThuc) + ',' AS [text()]
    							From 
    								(
    								SELECT qct2.ID_HoaDonLienQuan,
										case qct2.HinhThucThanhToan
											when 1 then concat(N'Tiền mặt (', FORMAT(qct2.TienThu, '#,#'),')')
											when 2 then concat(N'POS (',FORMAT(qct2.TienThu, '#,#'),')')
											when 3 then concat(N'Chuyển khoản (',FORMAT(qct2.TienThu, '#,#'),')')
											when 4 then concat(N'Thẻ giá trị (', FORMAT(qct2.TienThu, '#,#') ,')')
											when 5 then concat(N'Đổi điểm (', FORMAT(qct2.TienThu, '#,#'),')')
											when 6 then concat(N'Thu từ cọc (', FORMAT(qct2.TienThu, '#,#'),')')
											end as PhuongThuc											
    								FROM Quy_HoaDon_ChiTiet qct2
    								left join DM_TaiKhoanNganHang tk on qct2.ID_TaiKhoanNganHang = tk.ID
    								where qct2.ID_HoaDon like @ID_PhieuThuChi
    								) tbl1
    							Where tbl1.ID_HoaDonLienQuan = main1.ID_HoaDonLienQuan or (tbl1.ID_HoaDonLienQuan is null and  main1.ID_HoaDonLienQuan is null )
    							For XML PATH ('')
    						) PThuc_SoQuy
    					From Quy_HoaDon_ChiTiet main1 group by main1.ID_HoaDonLienQuan
    				) [Main] 
    		
    	) QuyXML on qct.ID_HoaDonLienQuan = QuyXML.ID_HoaDonLienQuan or (qct.ID_HoaDonLienQuan is null and  QuyXML.ID_HoaDonLienQuan is null )		
		where qct.ID_HoaDon = @ID_PhieuThuChi
    	group by qct.ID_HoaDonLienQuan, hd.MaHoaDon,hd.ID_BaoHiem,hd.PhaiThanhToanBaoHiem, hd.NgayLapHoaDon, hd.PhaiThanhToan, qhd.PhieuDieuChinhCongNo

		-- get infor PhieuThu truoc do
		select hd.MaHoaDon, hd.NgayLapHoaDon, 
			0 as TongTienThu,
    		0 as TienThu,
			sum(qct.TienThu) as DaChi,
    		N'Đã thanh toán' as GhiChu,
			max(qhd.NgayLapHoaDon) as NgayLapPhieu,
			'00000000-0000-0000-0000-000000000000' as ID_DoiTuong,
			'00000000-0000-0000-0000-000000000000' as ID_KhoanThuChi,
    		'' as PhuongThuc
		into #temp2
    	from Quy_HoaDon_ChiTiet qct
		join Quy_HoaDon qhd on qhd.ID = qct.ID_HoaDon
    	join BH_HoaDon hd  on qct.ID_HoaDonLienQuan = hd.ID
		where qct.ID_HoaDonLienQuan in 
				( select qct3.ID_HoaDonLienQuan 
				from Quy_HoaDon qhd3
				join Quy_HoaDon_ChiTiet qct3 on qhd3.ID = qct3.ID_HoaDon
				where qhd3.ID like @ID_PhieuThuChi)
		and convert(varchar, qhd.NgayLapHoaDon, 120) < (select top 1 convert(varchar, NgayLapPhieu, 120) from #temp)
		and qhd.TrangThai='1'
		group by qct.ID_HoaDonLienQuan, hd.MaHoaDon, hd.NgayLapHoaDon, hd.PhaiThanhToan

		-- group and sum
		select  tblView.MaHoaDon, tblView.NgayLapHoaDon, 
				sum(tblView.TongTienThu) as TongTienThu,
				sum(tblView.TienThu) as TienThu,
				sum(tblView.DaChi) as DaChi,
				max(GhiChu) as GhiChu,
				max(PhuongThuc) as PhuongThuc,
				max(ID_DoiTuong) as ID_DoiTuong,
				max(ID_KhoanThuChi) as ID_KhoanThuChi
		from (select * from #temp
			union
			select * from #temp2) tblView
		group by tblView.MaHoaDon, tblView.NgayLapHoaDon
		order by tblView.NgayLapHoaDon desc
END");

            Sql(@"ALTER PROCEDURE [dbo].[TinhLuongOT]
	@NgayCongChuan int,
	@tblCongThuCong CongThuCong readonly,
	@tblThietLapLuong ThietLapLuong readonly
AS
BEGIN
	
	SET NOCOUNT ON;	

		--declare @IDChiNhanhs varchar(max)='d93b17ea-89b9-4ecf-b242-d03b8cde71de'
		--declare @FromDate datetime='2020-08-01'
		--declare @ToDate datetime='2020-08-31'

		--declare @tblCongThuCong CongThuCong
		--insert into @tblCongThuCong
		--exec dbo.GetChiTietCongThuCong @IDChiNhanhs,'', @FromDate, @ToDate

		--declare @tblThietLapLuong ThietLapLuong
		--insert into @tblThietLapLuong
		--exec GetNS_ThietLapLuong @IDChiNhanhs,'', @FromDate, @ToDate

		---- ===== OT theo ngay ======================
		declare @thietlapOTNgay table (ID_NhanVien uniqueidentifier, ID uniqueidentifier, TenLoaiLuong nvarchar(max), LoaiLuong int, LuongCoBan float,  HeSo int, NgayApDung datetime, NgayKetThuc datetime)
		insert into @thietlapOTNgay
		select *	
		from @tblThietLapLuong pc 		
		where pc.LoaiLuong = 2

		select  a.*,
				case when LaPhanTram= 0 then GiaTri else (SoTien/@NgayCongChuan/8) * GiaTri /100 end as Luong1GioCong			
			into #temp1					
			from
					(select TenNhanVien, bs.ID_CaLamViec, bs.TenCa, bs.TongGioCong1Ca, bs.ID_NhanVien,
						bs.NgayCham, bs.LoaiNgay, bs.KyHieuCong, bs.SoGioOT, bs.Thu,
						pc.SoTien,
					
						pc.LoaiLuong,
						pc.HeSo,

						case when ngayle.ID is null then -- 0.chunhat, 6.thu7, 1.ngaynghi, 2.ngayle, 3.ngaythuong  			
							case bs.Thu
								when 6 then 6
								when 0 then 0
							else 3 end
						else bs.LoaiNgay end LoaiNgayThuong_Nghi,	

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_GiaTri
								when 0 then tlct.ThCN_GiaTri
							else tlct.LuongNgayThuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_GiaTri
								when 2 then  tlct.NgayLe_GiaTri
							else tlct.LuongNgayThuong end
						end as GiaTri,

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_LaPhanTramLuong
								when 0 then tlct.CN_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_LaPhanTramLuong
								when 2 then  tlct.NgayLe_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						end as LaPhanTram														
					from @tblCongThuCong bs
					join NS_Luong_PhuCap pc  on bs.ID_NhanVien= pc.ID_NhanVien
					join NS_ThietLapLuongChiTiet tlct on pc.ID= tlct.ID_LuongPhuCap 
					join NS_NhanVien nv on pc.ID_NhanVien= nv.ID		
					left join NS_NgayNghiLe ngayle on bs.NgayCham = ngayle.Ngay and ngayle.TrangThai='1'
					where tlct.LaOT= 1 and pc.LoaiLuong = 2
					and exists (select tl.ID_PhuCap from @tblThietLapLuong tl where pc.ID = tl.ID_PhuCap)
				)a

		declare @tblCongOTNgay table (ID_PhuCap uniqueidentifier,  ID_NhanVien uniqueidentifier, LoaiLuong float, LuongCoBan float, HeSo int,NgayApDung datetime, NgayKetThuc datetime,
		ID_CaLamViec uniqueidentifier, TenCa nvarchar(100), TongGioCong1Ca float, 
		LuongOT float, NgayCham datetime)			
		
		declare @otngayID_NhanVien uniqueidentifier, @otngayID_PhuCap uniqueidentifier, @otngayTenLoaiLuong nvarchar(max),
		@otngayLoaiLuong int,@otngayLuongCoBan float, @otngayHeSo int, @otngayNgayApDung datetime, @otngayNgayKetThuc datetime

		DECLARE curLuong CURSOR SCROLL LOCAL FOR
    		select *
    		from @thietlapOTNgay
		OPEN curLuong -- cur 1
    	FETCH FIRST FROM curLuong
    	INTO @otngayID_NhanVien, @otngayID_PhuCap, @otngayTenLoaiLuong, @otngayLoaiLuong, @otngayLuongCoBan, @otngayHeSo, @otngayNgayApDung, @otngayNgayKetThuc
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
				insert into @tblCongOTNgay
				select @otngayID_PhuCap,@otngayID_NhanVien, @otngayLoaiLuong,@otngayLuongCoBan,@otngayHeSo,@otngayNgayApDung, @otngayNgayKetThuc,
					tmp.ID_CaLamViec, tmp.TenCa, tmp.TongGioCong1Ca,
					tmp.SoGioOT * Luong1GioCong as LuongOT,
					tmp.NgayCham
				from #temp1 tmp
				where tmp.ID_NhanVien = @otngayID_NhanVien and tmp.NgayCham >= @otngayNgayApDung and (@otngayNgayKetThuc is null OR tmp.NgayCham <= @otngayNgayKetThuc )  								
				FETCH NEXT FROM curLuong 
				INTO @otngayID_NhanVien, @otngayID_PhuCap, @otngayTenLoaiLuong, @otngayLoaiLuong, @otngayLuongCoBan, @otngayHeSo, @otngayNgayApDung, @otngayNgayKetThuc
			END;
			CLOSE curLuong  
    		DEALLOCATE curLuong 	


			--- ======= OT Theo Ca =================
		declare @thietlapOTCa table (ID_NhanVien uniqueidentifier, ID uniqueidentifier, TenLoaiLuong nvarchar(max), LoaiLuong int, LuongCoBan float,  HeSo int, NgayApDung datetime, NgayKetThuc datetime)
		insert into @thietlapOTCa
		select *	
		from @tblThietLapLuong pc 		
		where pc.LoaiLuong = 3
		
		-- get cong OT ca
		select  a.*,				
				case when LaPhanTram = 0 then GiaTri else case when LuongTheoCa is null then SoTien/TongGioCong1Ca * GiaTri/100
					else LuongTheoCa/TongGioCong1Ca* GiaTri/100 end end as Luong1GioCong
			into #temp2					
			from
				(select bs.ID_CaLamViec, bs.TenCa, bs.TongGioCong1Ca, bs.ID_NhanVien,
					bs.NgayCham, bs.LoaiNgay, bs.KyHieuCong, bs.SoGioOT, bs.Thu,
					pc.SoTien,
					theoca.LuongTheoCa,
					pc.LoaiLuong,
					pc.HeSo,
					case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_GiaTri
								when 0 then tlct.ThCN_GiaTri
							else tlct.LuongNgayThuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_GiaTri
								when 2 then  tlct.NgayLe_GiaTri
							else tlct.LuongNgayThuong end
						end as GiaTri,

						case when ngayle.ID is null then   			
							case bs.Thu
								when 6 then tlct.Thu7_LaPhanTramLuong
								when 0 then tlct.CN_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						else 
							case bs.LoaiNgay 
								when 1 then tlct.NgayNghi_LaPhanTramLuong
								when 2 then  tlct.NgayLe_LaPhanTramLuong
							else tlct.NgayThuong_LaPhanTramLuong end
						end as LaPhanTram	
				from @tblCongThuCong bs
				join NS_Luong_PhuCap pc  on bs.ID_NhanVien= pc.ID_NhanVien
				join NS_ThietLapLuongChiTiet tlct on pc.ID= tlct.ID_LuongPhuCap -- luongot
				left join NS_NgayNghiLe ngayle on bs.NgayCham = ngayle.Ngay and ngayle.TrangThai='1'
				left join (select tlca.LuongNgayThuong as LuongTheoCa, tlca.ID_CaLamViec, pca.ID_NhanVien -- get luongcoban
						from NS_Luong_PhuCap pca
						join NS_ThietLapLuongChiTiet tlca on pca.ID= tlca.ID_LuongPhuCap 
						where tlca.LaOT= 0
						) theoca on pc.ID_NhanVien= theoca.ID_NhanVien and bs.ID_CaLamViec= theoca.ID_CaLamViec
				where tlct.LaOT= 1
				and pc.LoaiLuong = 3		
				and exists (select tl.ID_PhuCap from @tblThietLapLuong tl where pc.ID = tl.ID_PhuCap)
				) a			

		declare @tblCongOTCa table (ID_PhuCap uniqueidentifier,  ID_NhanVien uniqueidentifier, LoaiLuong float, LuongCoBan float, HeSo int,NgayApDung datetime, NgayKetThuc datetime,
		ID_CaLamViec uniqueidentifier, TenCa nvarchar(100), TongGioCong1Ca float, 
		LuongOT float, NgayCham datetime)				
	
		declare @ID_NhanVien uniqueidentifier, @ID_PhuCap uniqueidentifier, @TenLoaiLuong nvarchar(max), @LoaiLuong int,@LuongCoBan float, @HeSo int, @NgayApDung datetime, @NgayKetThuc datetime

		DECLARE curLuong CURSOR SCROLL LOCAL FOR
    		select *
    		from @thietlapOTCa
		OPEN curLuong
    	FETCH FIRST FROM curLuong
    	INTO @ID_NhanVien, @ID_PhuCap, @TenLoaiLuong, @LoaiLuong, @LuongCoBan, @HeSo, @NgayApDung, @NgayKetThuc
    		WHILE @@FETCH_STATUS = 0
    		BEGIN
				insert into @tblCongOTCa
				select @ID_PhuCap,@ID_NhanVien, @LoaiLuong,@LuongCoBan,@HeSo,@NgayApDung, @NgayKetThuc,
					tmp.ID_CaLamViec, tmp.TenCa, tmp.TongGioCong1Ca,
					tmp.SoGioOT * Luong1GioCong as LuongOT,
					tmp.NgayCham
				from #temp2 tmp
				where tmp.ID_NhanVien = @ID_NhanVien and tmp.NgayCham >= @NgayApDung and (@NgayKetThuc is null OR tmp.NgayCham <= @NgayKetThuc )  								
				FETCH NEXT FROM curLuong 
				INTO @ID_NhanVien, @ID_PhuCap, @TenLoaiLuong, @LoaiLuong, @LuongCoBan, @HeSo, @NgayApDung, @NgayKetThuc
			END;
			CLOSE curLuong  
    		DEALLOCATE curLuong 	

			--select thu, TenCa, SoGioOT, Luong1GioCong, LuongTheoCa, SoTien, NgayCham from #temp2 where ID_NhanVien='D559BADC-83AE-407C-8A79-BC160DF5C73A' order by NgayCham

		select ID_NhanVien, SUM(LuongOT) as LuongOT
		from
			(select ID_NhanVien, LuongOT from @tblCongOTNgay
			union all
			select ID_NhanVien, LuongOT from @tblCongOTCa
			) luongot group by luongot.ID_NhanVien
			
END");

            Sql(@"ALTER PROCEDURE [dbo].[UpdateHD_UpdateLichBaoDuong]
    @ID_HoaDon [uniqueidentifier],
    @IDHangHoas [nvarchar](max),
    @NgayLapHDOld [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @dtNow datetime = format( getdate(),'yyyy-MM-dd')
    	declare @SoKmMacDinhNgay int= 30, @countSC int =0;
    
    	declare @NgayLapHoaDon datetime , @ID_PhieuTiepNhan uniqueidentifier, @SoKmGanNhat float, @ID_Xe uniqueidentifier, @Now_SoKmVao int,  @Now_NgayVaoXuong datetime
    	select @ID_PhieuTiepNhan = ID_PhieuTiepNhan, @NgayLapHoaDon = NgayLapHoaDon from BH_HoaDon where id= @ID_HoaDon
    
    	---- get thongtin tiepnhan hientai
    	select @ID_Xe= ID_Xe, @Now_SoKmVao = isnull(SoKmVao,0), @Now_NgayVaoXuong = NgayVaoXuong from Gara_PhieuTiepNhan where ID = @ID_PhieuTiepNhan
    
    	---- get thongtin tiepnhan gan nhat
    	declare @NgayXuatXuong_GanNhat datetime , @SoKmRa_GanNhat float
    	select top 1 @NgayXuatXuong_GanNhat = isnull(NgayXuatXuong, NgayVaoXuong) ,  @SoKmRa_GanNhat= ISNULL(SoKmRa,0) 
    	from Gara_PhieuTiepNhan where isnull(NgayXuatXuong, NgayVaoXuong) < @Now_NgayVaoXuong and ID_Xe= @ID_Xe 
    	order by NgayVaoXuong 
    
    	
    	if @NgayXuatXuong_GanNhat is not null
    		begin
    			set @SoKmMacDinhNgay = CEILING( iif(@Now_SoKmVao - @SoKmRa_GanNhat=0,1, @Now_SoKmVao -@SoKmRa_GanNhat)/ iif(DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)=0,1,DATEDIFF(day, @NgayXuatXuong_GanNhat, @Now_NgayVaoXuong)))
    		end
    
    	---- get hang baoduong nhung bi xoa khi capnhat hoadon
    	declare @tblHH table(ID_HangHoa uniqueidentifier)
    	insert into @tblHH
    	select Name from dbo.splitstring(@IDHangHoas)
    
    	---- get caidat baoduong
    	select 
    		distinct bd.LoaiGiaTri,
    			bd.ID_HangHoa, 
    			bd.LanBaoDuong, 
    			iif(bd.BaoDuongLapDinhKy=1, bd.GiaTri,0) as GiaTriLap,
    			(select dbo.BaoDuong_GetTongGiaTriNhac(bd.LanBaoDuong,bd.ID_HangHoa)) as GiaTri,				
    			bd.BaoDuongLapDinhKy
    	into #tmpPhuTung
    	from DM_HangHoa_BaoDuongChiTiet bd
    	join @tblHH hh on bd.ID_HangHoa= hh.ID_HangHoa
    
    	select *
    		into #tnGanNhat
    		from(
    			select tn.ID_Xe, tn.NgayVaoXuong, tn.SoKmRa, qd.ID_HangHoa, ROW_NUMBER() over( partition by qd.ID_HangHoa order by tn.NgayVaoXuong) as RN		
    			from Gara_PhieuTiepNhan tn
    			join BH_HoaDon hd on tn.ID= hd.ID_PhieuTiepNhan
    			join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
    			join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    			where hd.ID!=@ID_HoaDon
    			and tn.ID_Xe= @ID_Xe
    			and tn.TrangThai !=0
    			and exists (
    			select ID_HangHoa from @tblHH pt where qd.ID_HangHoa= pt.ID_HangHoa
    			)			
    		) a where RN= 1
    
    
    
    	   ---- get cthd new (after update)
    	   select *
    	   into #ctNew
    	   from(
    		   select qd.ID_HangHoa,
    				ct.ID_LichBaoDuong,
    				ROW_NUMBER() over(partition by qd.ID_HangHoa order by ct.ID_LichBaoDuong desc, qd.ID_HangHoa) as RN
    		   from BH_HoaDon_ChiTiet ct
    		   join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID
    		   where ct.ID_HoaDon= @ID_HoaDon
    	  ) a where a.RN= 1
    
    	  ---- phụ tùng mua mới, và đã dc setup lịch bảo dưỡng, nhưng bị xóa khi cập nhật hóa đơn
    	  ---- find cthd was delete: update TrangThai = 0 (exist in baoduong, but not exist in cthd)
    	  update lichnew set lichnew.TrangThai= 0
    	  from Gara_LichBaoDuong lichnew
    	  where exists (
    	  select lich.ID
    	  from Gara_LichBaoDuong lich
    	  where lich.ID_HoaDon= @ID_HoaDon
    	  and not exists (select ID_HangHoa from #ctNew ct where lich.ID_HangHoa = ct.ID_HangHoa)
    	  and lich.ID= lichnew.ID
    	  )
    
    
    ---- phụ tùng chuyen tu baoduong ---> khong baoduong
    
    		select *
    		into #lichOld
    		from
    		(
    		---- so sanh ngaylaphdold voi lich nhac baoduong gan nhat
    		---- neu > : xoa + insert
    			select a.*,
    				iif(DATEDIFF(day,a.NgayLapHoaDon,@NgayLapHDOld) > 0,1,0) as isUpdate
    			from
    			(
    			
    				select lich.*, 
    					hd.NgayLapHoaDon,
    					ROW_NUMBER() over (partition by lich.ID_HangHoa order by hd.NgayLapHoaDon desc) as RN
    				from Gara_LichBaoDuong lich
    				join BH_HoaDon hd on lich.ID_HoaDon= hd.ID
    				where lich.ID_Xe = @ID_Xe
    				and lich.TrangThai!=0
    				and lich.ID_HoaDon= @ID_HoaDon
    				and exists (select ID_HangHoa from @tblHH hh where lich.ID_HangHoa= hh.ID_HangHoa) 
    			) a 		
    			where a.RN= 1
    		) b where b.isUpdate= 1
    
    		delete lich
    		from Gara_LichBaoDuong lich
    		where lich.TrangThai= 1
    		and exists (select id from #lichOld old where lich.ID = old.ID)
    
    
    		insert into Gara_LichBaoDuong (ID, ID_HangHoa, ID_HoaDon, ID_Xe, LanBaoDuong, SoKmBaoDuong, NgayBaoDuongDuKien, TrangThai, NgayTao)
    			select NEWID() as ID, a.ID_HangHoa, @ID_HoaDon, @ID_Xe, a.LanBaoDuong,
    				a.SoKmBaoDuong, a.NgayBaoDuongDuKien, a.TrangThai, a.NgayTao
    			from
    			(
    				select  pt.ID_HangHoa, pt.LanBaoDuong,
    				case pt.LoaiGiaTri
    					when 4 then @Now_SoKmVao + pt.GiaTri 
    					else 0 end as SoKmBaoDuong,
    				case when pt.LoaiGiaTri = 4 then DATEADD(day, CEILING( pt.GiaTri/@SoKmMacDinhNgay), @NgayLapHoaDon)				
    					else  DATEADD(day, pt.GiaTri, @NgayLapHoaDon)									
    				end as NgayBaoDuongDuKien,
    				1 as TrangThai,
    				GETDATE() as NgayTao
    				from #tmpPhuTung pt				
    			) a where a.NgayBaoDuongDuKien >= @dtNow --- chi insert neu lich > ngayhientai
END");

            Sql(@"ALTER FUNCTION [dbo].[DiscountSale_NVienDichVu]
(	
	@IDChiNhanhs varchar(max),
	@FromDate datetime,
	@ToDate datetime,
	@IDNhanVien varchar(40)
)
RETURNS TABLE 
AS
RETURN 
(
	select  2 as LoaiNhanVienApDung, tblNVienDV.ID_NhanVien, tblNVienDV.DoanhThu, 
	case when tblNVienDV.LaPhanTram = 1 then DoanhThu * GiaTriChietKhau / 100 else  GiaTriChietKhau end as HoaHongDoanhThu,
	tblNVienDV.ID
from
(
	select b.ID_NhanVien, b.DoanhThu, ckct.GiaTriChietKhau,ckct.LaPhanTram,ckct.ID,
	ROW_NUMBER() over (PARTITION  by b.ID_NhanVien order by ckct.DoanhThuTu desc)as Rn
	from
	(
			select  a.ID_NhanVien, sum(a.DoanhThu) - sum(a.GiaTriTra) as DoanhThu, a.ID_ChietKhauDoanhThu
			from
			(

				select 
					ckdtnv.ID_NhanVien , 
					nvth.ID_ChiTietHoaDon, 
					hd.MaHoaDon,   							
					---- HeSo * hoahong truoc/sau CK - phiDV
					iif(nvth.HeSo is null,1,nvth.HeSo) * (case when iif(nvth.TinhHoaHongTruocCK is null,0,nvth.TinhHoaHongTruocCK) = 1 
							then cthd.SoLuong * cthd.DonGia
							else cthd.SoLuong * (cthd.DonGia - cthd.TienChietKhau)
							end )
					- iif(hd.LoaiHoaDon=19,0, case when hh.ChiPhiTinhTheoPT =1 then cthd.SoLuong * cthd.DonGia * hh.ChiPhiThucHien/100
							else hh.ChiPhiThucHien * cthd.SoLuong end) as DoanhThu,
    				0 as GiaTriTra,
					ckdtnv.ID_ChietKhauDoanhThu, 
					ckdt.ApDungTuNgay,
					ckdt.ApDungDenNgay
				from ChietKhauDoanhThu ckdt
				join ChietKhauDoanhThu_NhanVien ckdtnv on ckdt.ID  = ckdtnv.ID_ChietKhauDoanhThu		
				join BH_NhanVienThucHien nvth on ckdtnv.ID_NhanVien = nvth.ID_NhanVien 
				join BH_HoaDon_ChiTiet cthd on nvth.ID_ChiTietHoaDon = cthd.ID 
				join DonViQuiDoi qd on cthd.ID_DonViQuiDoi = qd.ID
				join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
				join BH_HoaDon hd on cthd.ID_HoaDon= hd.ID
				and ckdt.ID_DonVi = hd.ID_DonVi and (ckdt.ApDungTuNgay <= hd.NgayLapHoaDon 
									and (Dateadd(day, 1,ckdt.ApDungDenNgay) >= hd.NgayLapHoaDon or ckdt.ApDungDenNgay is null))
				where hd.ChoThanhToan= 0 
				and exists (select Name from dbo.splitstring(@IDChiNhanhs) dv where hd.ID_DonVi= dv.Name)
				and hd.LoaiHoaDon in (1,19,22,25)
				and ckdt.LoaiNhanVienApDung= 2
				and hd.NgayLapHoaDon >= @FromDate  and hd.NgayLapHoaDon < @ToDate
				and cthd.ChatLieu!=4
				and nvth.ID_NhanVien like @IDNhanVien
				and ckdt.TrangThai= 1

				union all


				--- trahang
				select  ckdtnv.ID_NhanVien ,
					nvth.ID_ChiTietHoaDon,
					hdt.MaHoaDon,    								
    				0 as DoanhThu, 									
    				cthd.ThanhTien  as GiaTriTra,
    				ckdtnv.ID_ChietKhauDoanhThu,
					ckdt.ApDungTuNgay, 
					ckdt.ApDungDenNgay
    			from ChietKhauDoanhThu ckdt
    			join ChietKhauDoanhThu_NhanVien ckdtnv on ckdt.ID  = ckdtnv.ID_ChietKhauDoanhThu		
				join BH_NhanVienThucHien nvth on ckdtnv.ID_NhanVien = nvth.ID_NhanVien 
				join BH_HoaDon_ChiTiet cthd on nvth.ID_ChiTietHoaDon = cthd.ID 
    			join BH_HoaDon hdt on cthd.ID_HoaDon = hdt.ID 
				and (ckdt.ApDungTuNgay <= hdt.NgayLapHoaDon and (Dateadd(day, 1,ckdt.ApDungDenNgay) >= hdt.NgayLapHoaDon or ckdt.ApDungDenNgay is null))
    			left join Quy_HoaDon_ChiTiet qhdct on qhdct.ID_HoaDonLienQuan = hdt.ID
    			left join Quy_HoaDon qhd on qhdct.ID_HoaDon = qhd.ID
    			where 
    			exists (select Name from dbo.splitstring(@IDChiNhanhs) dv where hdt.ID_DonVi= dv.Name)
    			and ckdt.LoaiNhanVienApDung=2
    			and hdt.ChoThanhToan = '0' and hdt.LoaiHoaDon = 6
    			and hdt.NgayLapHoaDon >= @FromDate and hdt.NgayLapHoaDon < @ToDate
				and (qhd.TrangThai is null or qhd.TrangThai != 0)
				and nvth.ID_NhanVien like @IDNhanVien
				and ckdt.TrangThai= 1
			) a group by a.ID_NhanVien, a.ID_ChietKhauDoanhThu
	) b 
join ChietKhauDoanhThu_ChiTiet ckct on b.ID_ChietKhauDoanhThu = ckct.ID_ChietKhauDoanhThu 
and (b.DoanhThu >= ckct.DoanhThuTu) 								
)tblNVienDV where tblNVienDV.Rn= 1
)
");
        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[BaoCaoHoatDongXe_ChiTiet]");
			DropStoredProcedure("[dbo].[BaoCaoHoatDongXe_TongHop]");
			DropStoredProcedure("[dbo].[GetInforHoaDon_ByID]");
            DropStoredProcedure("[dbo].[GetListImgInvoice_byCus]");
            DropStoredProcedure("[dbo].[HuyPhieuThu_UpdateTrangThaiCong]");
        }
    }
}
