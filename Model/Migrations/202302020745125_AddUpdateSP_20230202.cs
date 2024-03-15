namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20230202 : DbMigration
    {
        public override void Up()
        {
            Sql(@"ALTER FUNCTION [dbo].[FN_GetMaHangHoa]
(
	@LoaiHangHoa int
)
RETURNS nvarchar(50)
AS
BEGIN
	DECLARE @MaHangHoa varchar(5);
    DECLARE @Return float = 0

	if @LoaiHangHoa = 1
		set @MaHangHoa ='HH'
	if @LoaiHangHoa = 2
		set @MaHangHoa ='DV'
	if @LoaiHangHoa = 3
		set @MaHangHoa ='CB'

	SELECT @Return = MAX(CAST (dbo.udf_GetNumeric(MaHangHoa) AS float))
    FROM DonViQuiDoi
	WHERE MaHangHoa like @MaHangHoa +'%'
	--and CHARINDEX('Copy',MaHangHoa)= 0 
	and CHARINDEX('_',MaHangHoa)= 0

	RETURN concat(@MaHangHoa,FORMAT(@Return + 1, 'F0'))

END
");

			CreateStoredProcedure(name: "[dbo].[LoadDanhMuc_KhachHangNhaCungCap]", parametersAction: p => new
			{
				IDChiNhanhs = p.String(defaultValue: "d93b17ea-89b9-4ecf-b242-d03b8cde71de"),
				LoaiDoiTuong = p.Int(defaultValue: 1),
				IDNhomKhachs = p.String(defaultValue: ""),
				TongBan_FromDate = p.DateTime(defaultValue: null),
				TongBan_ToDate = p.DateTime(defaultValue: null),
				NgayTao_FromDate = p.DateTime(defaultValue: null),
				NgayTao_ToDate = p.DateTime(defaultValue: null),
				TextSearch = p.String(defaultValue: " "),
				Where = p.String(defaultValue: " "),
				ColumnSort = p.String(defaultValue: "NgayTao", maxLength: 40),
				SortBy = p.String(defaultValue: "DESC", maxLength: 40),
				CurrentPage = p.Int(defaultValue: 0),
				PageSize = p.Int(defaultValue: 20)
			}, body: @"SET NOCOUNT ON;
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
				set @sql1= CONCAT(@sql1, N' 
				INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch_In, '' '') where Name!='''';
    			Select @count =  (Select count(*) from @tblSearch);')

				set @whereLast = CONCAT(@whereLast,
				 N' and ((select count(Name) from @tblSearch b where 				
    				 tbl.Name_Phone like ''%''+b.Name+''%''    		
    				)=@count or @count=0)')
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
			isnull(a.NoHienTai,0) as NoHienTai,
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
					hd.PhaiThanhToan as GiaTriTra,
    				0 as DoanhThu,
					0 AS TienThu,
    				0 AS TienChi, 
    				0 AS SoLanMuaHang,
					0 as ThuTuThe,
					0 as PhiDichVu
    			FROM BH_HoaDon hd ',  @whereInvoice, N'  and hd.LoaiHoaDon in (6,4) ',
				
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
					@PageSize_In = @PageSize");

			CreateStoredProcedure(name: "[dbo].[GetHangCungLoai_byID]", parametersAction: p => new
			{
				ID_HangCungLoai = p.Guid(),
				IDChiNhanh = p.Guid()
			}, body: @"SET NOCOUNT ON;

		select 
			hh.*,
			qd.ID as ID_DonViQuiDoi,
			qd.MaHangHoa,
			nhom.TenNhomHangHoa as NhomHangHoa,					
			qd.Xoa,
			qd.TenDonViTinh,
			qd.ThuocTinhGiaTri,
			qd.GiaBan,
			ISNULL(tk.TonKho,0) as TonKho,
			isnull(gv.GiaVon,0) as GiaVon,
			case hh.LoaiHangHoa 
			when 1 then N'Hàng hóa'
			when 2 then N'Dịch vụ'
			when 3 then N'Combo'
		end as sLoaiHangHoa
		from
		(
		  select 
				hh.ID,				
				hh.ID_Xe,				
				hh.TenHangHoa,					
				hh.LaHangHoa,
				hh.GhiChu,
				hh.LaChaCungLoai,
				hh.DuocBanTrucTiep,
				hh.TheoDoi,
				hh.NgayTao,
				hh.ID_HangHoaCungLoai,
				hh.ID_NhomHang as ID_NhomHangHoa,
					
				iif(hh.LoaiHangHoa is null,iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,
				isnull(hh.SoPhutThucHien,0) as SoPhutThucHien,
				isnull(hh.DichVuTheoGio,0) as DichVuTheoGio,	
				isnull(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
				isnull(hh.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT,
				isnull(hh.DuocTichDiem,0) as DuocTichDiem,
				iif(hh.QuanLyTheoLoHang is null,'0', hh.QuanLyTheoLoHang) as QuanLyTheoLoHang,
				iif(hh.QuanLyBaoDuong is null,0, hh.QuanLyBaoDuong) as QuanLyBaoDuong,
				iif(hh.LoaiBaoDuong is null,0, hh.LoaiBaoDuong) as LoaiBaoDuong,
				iif(hh.SoKmBaoHanh is null,0, hh.SoKmBaoHanh) as SoKmBaoHanh,
				iif(hh.HoaHongTruocChietKhau is null,0, hh.HoaHongTruocChietKhau) as HoaHongTruocChietKhau,		
				isnull(hh.TonToiDa,0) as TonToiDa,
				isnull(hh.TonToiThieu,0) as TonToiThieu
		   from DM_HangHoa hh 
		   where ID_HangHoaCungLoai= @ID_HangCungLoai
		) hh
		left join DonViQuiDoi qd on qd.ID_HangHoa= hh.ID
		left join DM_NhomHangHoa nhom on hh.ID_NhomHangHoa= nhom.ID	
		left join DM_HangHoa_TonKho tk on qd.ID = tk.ID_DonViQuyDoi and tk.ID_DonVi= @IDChiNhanh
		left join DM_GiaVon gv on qd.ID = gv.ID_DonViQuiDoi and gv.ID_DonVi= @IDChiNhanh
		where qd.Xoa='0' and qd.LaDonViChuan='1'
		order by qd.MaHangHoa");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_NhatKySuDungTongHop]
    @Text_Search [nvarchar](max),    
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LaHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
	@ThoiHan [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@Text_Search, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	declare @dtNow datetime = getdate()

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
		GiamGiaHD float)
	insert into @tblCTMua
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,@timeStart,@timeEnd

			select 
				b.MaHangHoa, 
				b.TenHangHoa, 
				b.MaLoHang as TenLoHang,
				b.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				CONCAT(b.TenHangHoa, b.ThuocTinhGiaTri) as TenHangHoaFull,
				b.TenDonViTinh,
				b.TenNhomHang,				
				b.MaDoiTuong as MaKhachHang,
				b.TenDoiTuong as TenKhachHang,
				b.DienThoai, 
				b.GioiTinh, 
				b.TenNguonKhach, 
				b.NhomKhachHang,
				b.NguoiGioiThieu,
				sum(SoLuong) as SoLuongMua,
				sum(SoLuongTra) as SoLuongTra,
				sum(SoLuongSuDung) as SoLuongSuDung,
				round(sum(SoLuong) - sum(SoLuongTra) -  sum(SoLuongSuDung),2) as SoLuongConLai
				from
				(
			
					select 
						ctm.ID_HoaDon,
						ctm.MaHoaDon,
						ctm.NgayLapHoaDon,
						ctm.NgayApDungGoiDV,
						ctm.HanSuDungGoiDV,
						dt.MaDoiTuong,
						dt.TenDoiTuong,
						dt.DienThoai,
						Case when dt.GioiTinhNam = 1 then N'Nam' else N'Nữ' end as GioiTinh,
						gt.TenDoiTuong as NguoiGioiThieu,
						nk.TenNguonKhach,
						isnull(dt.TenNhomDoiTuongs, N'Nhóm mặc định') as NhomKhachHang ,
						iif( hh.ID_NhomHang is null, '00000000-0000-0000-0000-000000000000',hh.ID_NhomHang) as ID_NhomHang,
						iif(@dtNow <=ctm.HanSuDungGoiDV,1,0) as ThoiHan,						
						ctm.SoLuong,
						ctm.DonGia,
						ctm.TienChietKhau,
						ctm.ThanhTien,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,						
						ctm.SoLuong- isnull(tbl.SoLuongTra,0) - isnull(tbl.SoLuongSuDung,0)  as SoLuongConLai,
						qd.MaHangHoa,
						qd.TenDonViTinh,
						hh.TenHangHoa,
						qd.ThuocTinhGiaTri,
						lo.MaLoHang,
						nhom.TenNhomHangHoa as TenNhomHang
					from @tblCTMua ctm
					inner join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
					inner join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
					left join DM_LoHang lo on ctm.ID_LoHang= lo.ID
					left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
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
							join @tblCTMua ctm on ct.ID_ChiTietGoiDV = ctm.ID
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon in (1,25)
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
							

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
							join @tblCTMua ctm on ct.ID_ChiTietGoiDV = ctm.ID
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)							
							)tblSD group by tblSD.ID_ChiTietGoiDV

					) tbl on ctm.ID= tbl.ID_ChiTietGoiDV
				where hh.LaHangHoa like @LaHangHoa
    			and hh.TheoDoi like @TheoDoi
    			and qd.Xoa like @TrangThai
				and (@ID_NhomHang is null or exists (select ID from dbo.GetListNhomHangHoa(@ID_NhomHang) nhomS where nhom.ID= nhomS.ID))
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
				group by b.MaHangHoa, b.TenHangHoa, b.ThuocTinhGiaTri,b.TenDonViTinh, b.MaLoHang, b.TenNhomHang,				
				b.MaDoiTuong, b.TenDoiTuong, b.DienThoai, b.GioiTinh, b.TenNguonKhach, b.NhomKhachHang, b.NguoiGioiThieu
END");

            Sql(@"ALTER PROCEDURE [dbo].[DanhMucKhachHang_CongNo_Paging]
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @MaKH [nvarchar](max),
    @LoaiKH [int],
    @ID_NhomKhachHang [nvarchar](max),
    @timeStartKH [datetime],
    @timeEndKH [datetime],
    @CurrentPage [int],
    @PageSize [float],
    @Where [nvarchar](max),
    @SortBy [nvarchar](100)
AS
BEGIN
    set nocount on
    	
    	if @SortBy ='' set @SortBy = ' dt.NgayTao DESC'
    	if @Where='' set @Where= ''
    	else set @Where= ' AND '+ @Where 
    
    	declare @from int= @CurrentPage * @PageSize + 1  
    	declare @to int= (@CurrentPage + 1) * @PageSize 
    
    	declare @sql1 nvarchar(max)= concat('
    	declare @tblIDNhoms table (ID varchar(36)) 
    	declare @idNhomDT nvarchar(max) = ''', @ID_NhomKhachHang, '''
    
    	declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh select * from splitstring(''',@ID_ChiNhanh, ''')
    	
    	if @idNhomDT =''%%''
    		begin
				insert into @tblIDNhoms(ID) values (''00000000-0000-0000-0000-000000000000'')

    			-- check QuanLyKHTheochiNhanh
    			--declare @QLTheoCN bit = (select QuanLyKhachHangTheoDonVi from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID))  
				
				declare @QLTheoCN bit = 0;
				declare @countQL int=0;
				select distinct QuanLyKhachHangTheoDonVi into #temp from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID)
				set @countQL = (select COUNT(*) from #temp)
				if	@countQL= 1 
						set @QLTheoCN = (select QuanLyKhachHangTheoDonVi from #temp)
				
    			if @QLTheoCN = 1
    				begin									
    					insert into @tblIDNhoms(ID)
    					select *  from (
    						-- get Nhom not not exist in NhomDoiTuong_DonVi
    						select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    						where not exists (select ID_NhomDoiTuong from NhomDoiTuong_DonVi where ID_NhomDoiTuong= nhom.ID) 
    						and LoaiDoiTuong = ',@LoaiKH ,'
    						union all
    						-- get Nhom at this ChiNhanh
    						select convert(varchar(36),ID_NhomDoiTuong)  from NhomDoiTuong_DonVi where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) ) tbl
    				end
    			else
    				begin				
    				-- insert all
    				insert into @tblIDNhoms(ID)
    				select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    				where LoaiDoiTuong = ',@LoaiKH, ' 
    				end		
    		end
    	else
    		begin
    			set @idNhomDT = REPLACE( @idNhomDT,''%'','''')
    			insert into @tblIDNhoms(ID) values (@idNhomDT)
    		end
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    	DECLARE @count int;
    	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](''',@MaKH,''', '' '') where Name!='''';
    	Select @count =  (Select count(*) from @tblSearchString);')
    
    	declare @sql2 nvarchar(max)= concat(' WITH Data_CTE ',
    									' AS ',
    									' ( ',
    
    N'SELECT  * 
    		FROM
    		(  
    			SELECT 
    		  dt.ID as ID,
    		  dt.MaDoiTuong, 
    			  case when dt.IDNhomDoiTuongs='''' then ''00000000-0000-0000-0000-000000000000'' 
				  else  ISNULL(dt.IDNhomDoiTuongs,''00000000-0000-0000-0000-000000000000'') end as ID_NhomDoiTuong,
    	      dt.TenDoiTuong,
    		  dt.TenDoiTuong_KhongDau,
    		  dt.TenDoiTuong_ChuCaiDau,
    			  dt.ID_TrangThai,
				   dt.TheoDoi,
    		  dt.GioiTinhNam,
    		  dt.NgaySinh_NgayTLap,
			   dt.NgayGiaoDichGanNhat,
    			  ISNULL(dt.DienThoai,'''') as DienThoai,
    			  ISNULL(dt.Email,'''') as Email,
    			  ISNULL(dt.DiaChi,'''') as DiaChi,
    			  ISNULL(dt.MaSoThue,'''') as MaSoThue,
				  dt.TaiKhoanNganHang,
    		  ISNULL(dt.GhiChu,'''') as GhiChu,
    		  dt.NgayTao,
    		  dt.DinhDang_NgaySinh,
    		  ISNULL(dt.NguoiTao,'''') as NguoiTao,
    		  dt.ID_NguonKhach,
    		  dt.ID_NhanVienPhuTrach,
    		  dt.ID_NguoiGioiThieu,
    			  dt.ID_DonVi, --- Collate Vietnamese_CI_AS
    		  dt.LaCaNhan,
    		  CAST(ISNULL(dt.TongTichDiem,0) as float) as TongTichDiem,
    			 case when right(rtrim(dt.TenNhomDoiTuongs),1) ='','' then LEFT(Rtrim(dt.TenNhomDoiTuongs),
				  len(dt.TenNhomDoiTuongs)-1) else				  
				  ISNULL(dt.TenNhomDoiTuongs, N''Nhóm mặc định'') end   as TenNhomDT,-- remove last coma
    		  dt.ID_TinhThanh,
    		  dt.ID_QuanHuyen,
    			ISNULL(dt.TrangThai_TheGiaTri,1) as TrangThai_TheGiaTri,
    	      CAST(ROUND(ISNULL(a.NoHienTai,0), 0) as float) as NoHienTai,
    		  CAST(ROUND(ISNULL(a.TongBan,0), 0) as float) as TongBan,
    		  CAST(ROUND(ISNULL(a.TongBanTruTraHang,0), 0) as float) as TongBanTruTraHang,
    		  CAST(ROUND(ISNULL(a.TongMua,0), 0) as float) as TongMua,
    		  CAST(ROUND(ISNULL(a.SoLanMuaHang,0), 0) as float) as SoLanMuaHang,
			  a.PhiDichVu,
			  isnull(a.NapCoc,0) as NapCoc,
			  isnull(a.SuDungCoc,0) as SuDungCoc,
			  isnull(a.SoDuCoc,0) as SoDuCoc,
    			CAST(0 as float) as TongNapThe , 
    			CAST(0 as float) as SuDungThe , 
    			CAST(0 as float) as HoanTraTheGiaTri , 
    			CAST(0 as float) as SoDuTheGiaTri , 
    			  concat(dt.MaDoiTuong,'' '',lower(dt.MaDoiTuong) ,'' '', dt.TenDoiTuong,'' '', dt.DienThoai,'' '', dt.TenDoiTuong_KhongDau)  as Name_Phone			
    		  FROM DM_DoiTuong dt  ','')
    	
    	declare @sql3 nvarchar(max)= concat('
    				LEFT JOIN (
    					SELECT tblThuChi.ID_DoiTuong,
    						SUM(ISNULL(tblThuChi.DoanhThu,0)) + SUM(ISNULL(tblThuChi.HoanTraThe,0)) + SUM(ISNULL(tblThuChi.TienChi,0)) + sum(ISNULL(tblThuChi.ThuTuThe,0))
							- sum(isnull(tblThuChi.PhiDichVu,0)) 
						- SUM(ISNULL(tblThuChi.TienThu,0)) - SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS NoHienTai,
    					SUM(ISNULL(tblThuChi.DoanhThu, 0)) as TongBan,
						sum(ISNULL(tblThuChi.ThuTuThe,0)) as ThuTuThe,
    					SUM(ISNULL(tblThuChi.DoanhThu,0)) -  SUM(ISNULL(tblThuChi.GiaTriTra,0)) AS TongBanTruTraHang,
    					SUM(ISNULL(tblThuChi.GiaTriTra, 0)) - SUM(ISNULL(tblThuChi.DoanhThu, 0)) as TongMua,
    					SUM(ISNULL(tblThuChi.SoLanMuaHang, 0)) as SoLanMuaHang,
						sum(isnull(tblThuChi.PhiDichVu,0)) as PhiDichVu,
						sum(isnull(tblThuChi.NapCoc,0)) as NapCoc,
						sum(isnull(tblThuChi.SuDungCoc,0)) as SuDungCoc,
						sum(isnull(tblThuChi.NapCoc,0))  - sum(isnull(tblThuChi.SuDungCoc,0)) as SoDuCoc
    				FROM
    				(
    					select 
							cp.ID_NhaCungCap as ID_DoiTuong,
							0 as GiaTriTra,
    						0 as DoanhThu,
							0 AS TienThu,
    						0 AS TienChi, 
    						0 AS SoLanMuaHang,
							0 as ThuTuThe,
							sum(cp.ThanhTien) as PhiDichVu,
							0 as HoanTraThe, --- hoantra lai sodu co trong TGT cho khach
							0 as NapCoc,
							0 as SuDungCoc
						from BH_HoaDon_ChiPhi cp
						join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
						where hd.ChoThanhToan = 0
						and exists (select * from @tblChiNhanh cn where hd.ID_DonVi= cn.ID)
						group by cp.ID_NhaCungCap

						union all

						---- hoantra sodu TGT cho khach (giam sodu TGT)
						SELECT 
    							bhd.ID_DoiTuong,    	
								0 as GiaTriTra,
    							0 as DoanhThu,
								0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								-sum(bhd.PhaiThanhToan) as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    					FROM BH_HoaDon bhd
						where bhd.LoaiHoaDon = 32 and bhd.ChoThanhToan = 0 
						and exists (select * from @tblChiNhanh cn where bhd.ID_DonVi= cn.ID)
						group by bhd.ID_DoiTuong

						union all
    						---- tongban
    						SELECT 
    							bhd.ID_DoiTuong,    	
								0 as GiaTriTra,
    							bhd.PhaiThanhToan as DoanhThu,
								0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    						FROM BH_HoaDon bhd
    						WHERE bhd.LoaiHoaDon in (1,7,19,25) AND bhd.ChoThanhToan = ''0'' 
    							AND bhd.NgayLapHoaDon between ''', @timeStart ,''' AND ''',@timeEnd,
    						''' AND exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) 

							
							union all
							---- doanhthu tuthe
							select 
								bhd.ID_DoiTuong,
								0 as GiaTriTra,
    							0 AS DoanhThu,
								0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								bhd.PhaiThanhToan as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
							from BH_HoaDon bhd
    						WHERE bhd.LoaiHoaDon = 22 AND bhd.ChoThanhToan = ''0'' 
    							AND bhd.NgayLapHoaDon between ''', @timeStart ,''' AND ''',@timeEnd,
    						''' AND exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) 						

						
    							 union all
    							-- gia tri trả từ bán hàng
    						SELECT bhd.ID_DoiTuong,
								bhd.PhaiThanhToan AS GiaTriTra,    							
    							0 AS DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi, 
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    						FROM BH_HoaDon bhd   						
    						WHERE bhd.LoaiHoaDon in (6,4, 13,14) AND bhd.ChoThanhToan = ''0''  
    							AND bhd.NgayLapHoaDon between ''', @timeStart ,''' AND ''',@timeEnd,		
    						''' AND exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) 
							   							
    							 union all
    
    							-- tienthu
    							SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							ISNULL(qhdct.TienThu,0) AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon   						
    						WHERE qhd.LoaiHoaDon = 11 AND  (qhd.TrangThai != ''0'' OR qhd.TrangThai is null)
							and qhdct.HinhThucThanhToan!= 6
							and (qhdct.LoaiThanhToan is null or qhdct.LoaiThanhToan != 3)
    						AND exists (select * from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID) 
    							AND qhd.NgayLapHoaDon between ''',@timeStart,''' AND ''',@timeEnd  ,


								---- datcoc ----
								''' union all
									
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								iif(qhd.LoaiHoaDon=12,qhdct.TienThu,-qhdct.TienThu) as NapCoc,
								0 as SuDungCoc
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon 							
    						WHERE (qhd.TrangThai != ''0'' OR qhd.TrangThai is null)
							and qhdct.LoaiThanhToan = 1
							AND exists (select * from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID) ',
    							
								---- sudungcoc ----
								' union all
									
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    							0 AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								iif(qhd.LoaiHoaDon=12,qhdct.TienThu,-qhdct.TienThu) as SuDungCoc
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon 							
    						WHERE (qhd.TrangThai != ''0'' OR qhd.TrangThai is null)
							and qhdct.HinhThucThanhToan = 6
							AND exists (select * from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID) ',

    							' union all
    
    							-- tienchi
    						SELECT 
    							qhdct.ID_DoiTuong,						
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    							ISNULL(qhdct.TienThu,0) AS TienChi,
    							0 AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    						FROM Quy_HoaDon qhd
    						JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon 							
    						WHERE qhd.LoaiHoaDon = 12 AND (qhd.TrangThai != ''0'' OR qhd.TrangThai is null)
							and qhdct.HinhThucThanhToan!= 6
							and (qhdct.LoaiThanhToan is null or qhdct.LoaiThanhToan != 3)
    							AND qhd.NgayLapHoaDon between ''',@timeStart,''' AND ''',@timeEnd  ,
    						''' AND exists (select * from @tblChiNhanh cn where qhd.ID_DonVi= cn.ID)' )
    
    declare @sql4 nvarchar(max)= concat(' Union All
    							-- solan mua hang
    						Select 
    							hd.ID_DoiTuong,
    							0 AS GiaTriTra,
    							0 AS DoanhThu,
    							0 AS TienThu,
    								0 as TienChi,
    							COUNT(*) AS SoLanMuaHang,
								0 as ThuTuThe,
								0 as PhiDichVu,
								0 as HoanTraThe,
								0 as NapCoc,
								0 as SuDungCoc
    						From BH_HoaDon hd 
    						where hd.LoaiHoaDon in (1,19,25)
    						and hd.ChoThanhToan = 0
    						AND hd.NgayLapHoaDon between ''',@timeStart,''' AND ''',@timeEnd ,
    						''' AND exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) 
    							 GROUP BY hd.ID_DoiTuong  	
    							)AS tblThuChi
    						GROUP BY tblThuChi.ID_DoiTuong
    				) a on dt.ID = a.ID_DoiTuong  					
    						WHERE  dt.loaidoituong =', @loaiKH  ,					
    						' and dt.NgayTao between ''',@timeStartKH, ''' and ''',@timeEndKH,
    							''' and ( MaDoiTuong like ''%', @MaKH, '%'' OR  TenDoiTuong like ''%',@MaKH, '%'' or TenDoiTuong_KhongDau like ''%',@MaKH, '%'' or DienThoai like ''%',@MaKH, '%'' or Email like ''%',@MaKH, '%'' )',
    						
    						  '', @Where, ')b
    				 where b.ID not like ''%00000000-0000-0000-0000-0000%''
    				 and EXISTS(SELECT Name FROM splitstring(b.ID_NhomDoiTuong) lstFromtbl inner JOIN @tblIDNhoms tblsearch ON lstFromtbl.Name = tblsearch.ID)
    				  ),
    			Count_CTE ',
    			' AS ',
    			' ( ',
    			' SELECT COUNT(*) AS TotalRow, CEILING(COUNT(*) / CAST(',@PageSize, ' as float )) as TotalPage ,
					SUM(TongBan) as TongBanAll,
					SUM(TongBanTruTraHang) as TongBanTruTraHangAll,
					SUM(TongTichDiem) as TongTichDiemAll,
					SUM(NoHienTai) as NoHienTaiAll,
					SUM(PhiDichVu) as TongPhiDichVu,
					SUM(NapCoc) as NapCocAll,
					SUM(SuDungCoc) as SuDungCocAll,
					SUM(SoDuCoc) as SoDuCocAll
					FROM Data_CTE ',
    			' ) ',
    			' SELECT dt.*, cte.TotalPage, cte.TotalRow, 
					cte.TongBanAll, 
					cte.TongBanTruTraHangAll,
					cte.TongTichDiemAll,
					cte.NoHienTaiAll,
					cte.TongPhiDichVu,
    				  ISNULL(trangthai.TenTrangThai,'''') as TrangThaiKhachHang,
    				  ISNULL(qh.TenQuanHuyen,'''') as PhuongXa,
    				  ISNULL(tt.TenTinhThanh,'''') as KhuVuc,
    				  ISNULL(dv.TenDonVi,'''') as ConTy,
    				  ISNULL(dv.SoDienThoai,'''') as DienThoaiChiNhanh,
    				  ISNULL(dv.DiaChi,'''') as DiaChiChiNhanh,
    				  ISNULL(nk.TenNguonKhach,'''') as TenNguonKhach,
    				  ISNULL(dt2.TenDoiTuong,'''') as NguoiGioiThieu,
					  ISNULL(nvpt.MaNhanVien,'''') as MaNVPhuTrach,
					 ISNULL(nvpt.TenNhanVien,'''') as TenNhanVienPhuTrach',
    			' FROM Data_CTE dt',
    			' CROSS JOIN Count_CTE cte',
    			' LEFT join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID ',
    		' LEFT join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID ',
    			' LEFT join DM_NguonKhachHang nk on dt.ID_NguonKhach = nk.ID ',
    			' LEFT join DM_DoiTuong dt2 on dt.ID_NguoiGioiThieu = dt2.ID ',
				' LEFT join NS_NhanVien nvpt on dt.ID_NhanVienPhuTrach = nvpt.ID ',
    		' LEFT join DM_DonVi dv on dt.ID_DonVi = dv.ID ',
    			' LEFT join DM_DoiTuong_TrangThai trangthai on dt.ID_TrangThai = trangthai.ID ',
    			' ORDER BY ',@SortBy,
    			' OFFSET (', @CurrentPage, ' * ', @PageSize ,') ROWS ',
    			' FETCH NEXT ', @PageSize , ' ROWS ONLY ')
    

    			exec (@sql1 + @sql2 + @sql3 + @sql4)
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetBangCongNhanVien]
    @IDChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [uniqueidentifier],
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
	

    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @IDChiNhanhs,'BangCong_XemDS_PhongBan','BangCong_XemDS_HeThong');
    
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
    
    	declare @tblca table(ID_CaLamViec uniqueidentifier)
    	if @IDCaLamViecs ='%%'
    		insert into @tblca
    		select ID from NS_CaLamViec
    	else
    		insert into @tblca
    		select Name from dbo.splitstring(@IDCaLamViecs);
    
    		with data_cte
    		as(
    
    		select nv.ID as ID_NhanVien, nv.MaNhanVien, nv.TenNhanVien,
			iif(nv.DaNghiViec='1', 0,isnull(nv.TrangThai,1)) as TrangThaiNV,
    			cast(congnv.CongNgayThuong as float) as CongChinh, cast(congnv.CongNgayNghiLe as float) as CongLamThem,
    			cast(congnv.OTNgayThuong as float) as OTNgayThuong, 
    			congnv.OTNgayNghiLe as OTNgayNghiLe,
    			cast(congnv.OTNgayThuong + congnv.OTNgayNghiLe as float) as SoGioOT,
    			cast(congnv.SoPhutDiMuon as float) as SoPhutDiMuon
    		from
    			(select cong.ID_NhanVien,
    				sum(cong.CongNgayThuong) as CongNgayThuong,
    				sum(CongNgayNghiLe) as CongNgayNghiLe,
    				sum(OTNgayThuong) as OTNgayThuong,
    				sum(OTNgayNghiLe) as OTNgayNghiLe,
    				sum(SoPhutDiMuon) as SoPhutDiMuon
    			from
    				(select bs.ID_ChamCongChiTiet, bs.ID_CaLamViec, ca.TongGioCong as TongGioCong1Ca, ca.TenCa, bs.ID_NhanVien,
    					bs.NgayCham, bs.LoaiNgay, bs.KyHieuCong, bs.Cong, bs.CongQuyDoi, bs.SoGioOT, bs.GioOTQuyDoi, bs.SoPhutDiMuon,
    					IIF(bs.LoaiNgay=0, bs.Cong,0) as CongNgayThuong,
    					IIF(bs.LoaiNgay!=0, bs.Cong,0) as CongNgayNghiLe,
    					IIF(bs.LoaiNgay=0, bs.SoGioOT,0) as OTNgayThuong,
    					IIF(bs.LoaiNgay!=0, bs.SoGioOT,0) as OTNgayNghiLe
    				from NS_CongBoSung bs
    				join NS_CaLamViec ca on bs.ID_CaLamViec = ca.ID
    				where NgayCham >= @FromDate and NgayCham <= @ToDate
    				and bs.TrangThai !=0
    				and exists(select ID from @tblNhanVien nv where bs.ID_NhanVien= nv.ID)
    				and (@IDCaLamViecs ='%%' or exists(select ID_CaLamViec from @tblca ca where bs.ID_CaLamViec= ca.ID_CaLamViec))
    				and exists(select Name from dbo.splitstring(@IDChiNhanhs) dv where bs.ID_DonVi= dv.Name)
    				) cong
    			group by cong.ID_NhanVien
    			) congnv
    			join NS_NhanVien nv on congnv.ID_NhanVien= nv.ID 
    		
    			WHERE ((select count(Name) from @tblSearchString b 
    				where nv.TenNhanVien like '%'+b.Name+'%'  						
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'
    				)=@count or @count=0)	
				and exists (select TrangThaiNV from @tblTrangThaiNV tt
							where iif(nv.DaNghiViec='1', 0,isnull(nv.TrangThai,1)) = tt.TrangThaiNV)
				and (@IDPhongBans ='' or 
				----- NV từng chấm công ở CN1, nay chuyển công tác sang CN2
					exists (select pb.ID from @tblPhong pb 
					join NS_QuaTrinhCongTac ct on pb.ID =  ct.ID_PhongBan
					where exists(select Name from dbo.splitstring(@IDChiNhanhs) dv where ct.ID_DonVi= dv.Name)))
    		),
    		count_cte
    		as
    		( SELECT COUNT(*) AS TotalRow, 
    			CEILING(COUNT(*) / CAST(@PageSize as float )) as TotalPage ,
    			cast(sum(CongChinh) as float) as TongCong,
    			cast(sum(CongLamThem) as float)as TongCongNgayNghi,
    			cast(sum(SoGioOT) as float) as TongOT,
    			cast(sum(SoPhutDiMuon) as float) as TongDiMuon
    
    		FROM Data_CTE
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaNhanVien
    		OFFSET (@CurrentPage * @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListLichHen_FullCalendar]
    @IDChiNhanhs [nvarchar](max),
    @IDLoaiTuVans [nvarchar](max),
    @IDNhanVienPhuTrachs [nvarchar](max),
    @TrangThaiCVs [nvarchar](20),
    @PhanLoai [nvarchar](20),
    @DoUuTien [nvarchar](4),
    @LoaiDoiTuong [nvarchar](10),
    @FromDate [datetime],
    @ToDate [datetime],
    @IDKhachHang [nvarchar](40),
	@TextSearch nvarchar(max),
	@CurrentPage int,
	@PageSize int
AS
BEGIN
    SET NOCOUNT ON;	

	declare @today datetime = format( DATEADD(SECOND, -1, getdate()),'yyyy-MM-dd')
	declare @SFromDate nvarchar(max) = CONVERT(varchar(14),  DATEADD(SECOND, -2, @FromDate),23)
	declare @SToDate nvarchar(max) = CONVERT(varchar(14), @ToDate,23)

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	declare @tblChiNhanh table(ID uniqueidentifier)
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@IDChiNhanhs)

	declare @tblTrangThai table(TrangThaiCV int)
	insert into @tblTrangThai
	select Name from dbo.splitstring(@TrangThaiCVs)

	CREATE TABLE #temp(	
		ID uniqueidentifier not null, 
    	Ma_TieuDe nvarchar(max),
    	ID_DonVi uniqueidentifier, 
    	ID_KhachHang uniqueidentifier, 
    	ID_LoaiTuVan uniqueidentifier,
    	ID_NhanVien uniqueidentifier,	
    	ID_NhanVienQuanLy uniqueidentifier,
    	NgayTao datetime,
    	NgayGio datetime,
    	NgayGioKetThuc datetime,	
    	NgayHoanThanh datetime,
		KieuLap int ,
		SoLanLap int,
		GiaTriLap varchar(max),
		TuanLap int,
		TrangThaiKetThuc int,
		GiaTriKetThuc varchar(max),
		SoLanDaHen int,
		TrangThai int,
		GhiChu nvarchar(max),
		NoiDung nvarchar(max),
		NguoiTao nvarchar(max),
		MucDoUuTien int,
		KetQua nvarchar(max),
		NhacNho int,
		KieuNhacNho int,
		ID_Parent uniqueidentifier,
		NgayCu datetime   	
)

CREATE TABLE #tempCur(	
		ID uniqueidentifier not null, 
    	Ma_TieuDe nvarchar(max),
    	ID_DonVi uniqueidentifier, 
    	ID_KhachHang uniqueidentifier, 
    	ID_LoaiTuVan uniqueidentifier,
    	ID_NhanVien uniqueidentifier,	
    	ID_NhanVienQuanLy uniqueidentifier,
    	NgayTao datetime,
    	NgayGio datetime,
    	NgayGioKetThuc datetime,	
    	NgayHoanThanh datetime,
		KieuLap int not null,
		SoLanLap int,
		GiaTriLap varchar(max),
		TuanLap int,
		TrangThaiKetThuc int,
		GiaTriKetThuc varchar(max),
		SoLanDaHen int,
		TrangThai int,
		GhiChu nvarchar(max),
		NoiDung nvarchar(max),
		NguoiTao nvarchar(max),
		MucDoUuTien int,
		KetQua nvarchar(max),
		NhacNho int,
		KieuNhacNho int,
		ID_Parent uniqueidentifier,
		NgayCu datetime   	
)

CREATE CLUSTERED INDEX calendar_KieLap ON #tempCur(KieuLap)

CREATE TABLE #temp2(	
		ID uniqueidentifier PRIMARY KEY,     	
		ID_Parent uniqueidentifier not null,
		NgayCu datetime   	,
		NgayCu_yyyyMMdd datetime
)
 

	declare @tblNVPhuTrach table(ID uniqueidentifier)
	insert into @tblNVPhuTrach
	select Name from dbo.splitstring(@IDNhanVienPhuTrachs)

	union all
	--- get lich hen cua NV da bi xoa
	select nv.ID from NS_NhanVien nv
	where nv.TrangThai='0'
	and exists (
		select cn.ID 
		from @tblChiNhanh cn
		join NS_QuaTrinhCongTac ct on ct.ID_DonVi = cn.ID
		where nv.ID = ct.ID_NhanVien
	)

	declare @tblLoaiCV table(ID uniqueidentifier)
	insert into @tblLoaiCV
	select Name from dbo.splitstring(@IDLoaiTuVans)
	
    
    declare @tblCalendar table(ID uniqueidentifier,Ma_TieuDe nvarchar (max), ID_DonVi uniqueidentifier, ID_KhachHang uniqueidentifier,ID_LoaiTuVan uniqueidentifier null,
    	ID_NhanVien uniqueidentifier, ID_NhanVienQuanLy uniqueidentifier null, 
    	NgayTao datetime,NgayHenGap datetime, NgayGioKetThuc datetime null,NgayHoanThanh datetime null,
    	TrangThai varchar(10), GhiChu nvarchar(max), NoiDung nvarchar(max), NguoiTao nvarchar(max),
    	MucDoUuTien int, KetQua nvarchar(max), NhacNho int null, KieuNhacNho int null,
    	KieuLap int null, SoLanLap int null, GiaTriLap nvarchar(max) null,TuanLap int null, TrangThaiKetThuc int null,GiaTriKetThuc nvarchar(max), 
    	ExistDB bit,ID_Parent uniqueidentifier null, NgayCu datetime null)	
    
	insert into #temp WITH(TABLOCKX)
    --- table LichHen
    select cs.ID, 
    	Ma_TieuDe,
    	cs.ID_DonVi, 
    	ID_KhachHang, 
    	ID_LoaiTuVan,
    	ID_NhanVien,	
    	ID_NhanVienQuanLy,
    	NgayTao,
    	NgayGio,
    	NgayGioKetThuc,	
    	NgayHoanThanh,
    	ISNULL(KieuLap,0) as KieuLap,
    	ISNULL(SoLanLap,0) as SoLanLap,
    	ISNULL(GiaTriLap,'') as GiaTriLap,
    	ISNULL(TuanLap,0) as TuanLap,
    	ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    	ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc, 	
    	ISNULL(a.SoLanDaHen,0) as SoLanDaHen,
    	TrangThai,
		ISNULL(GhiChu,'') as GhiChu,
    	ISNULL(NoiDung,'') as NoiDung,
    	NguoiTao,
    	2 as MucDoUuTien,
    	KetQua,
    	NhacNho, 
    	ISNULL(KieuNhacNho,0) as KieuNhacNho,
		case when cs.ID_Parent is null then cs.ID else cs.ID_Parent end as ID_Parent,
    	cs.NgayCu
    from ChamSocKhachHangs cs
    left join  
		( select ISNULL(ID_Parent,'00000000-0000-0000-0000-000000000000') as ID_Parent,
    		count(ID) as SoLanDaHen
    		from ChamSocKhachHangs
    		where PhanLoai = 3
    		group by ID_Parent) a 
			on cs.ID= a.ID_Parent
    where KieuLap is not null
    	and (TrangThaiKetThuc = 1 
    	OR (TrangThaiKetThuc = 2 and ISNULL(GiaTriKetThuc,'')  > @SFromDate)
    	OR (TrangThaiKetThuc = 3 and ISNULL(a.SoLanDaHen,0)  < ISNULL(GiaTriKetThuc,1)) 
		OR TrangThaiKetThuc is null
    	)	
    and PhanLoai = 3 
	and exists (select ID from @tblChiNhanh cn where cn.id = cs.ID_DonVi)
    
    -- get row was update (ID_Parent !=null)
	insert into #temp2  WITH(TABLOCKX)
    select ID, ID_Parent, NgayCu, format(NgayCu,'yyyy-MM-dd') as NgayCu_yyyyMMdd 
	from #temp where ID_Parent != ID

	insert into #tempCur WITH(TABLOCKX)
	select t1.* 
	from #temp t1
	left join #temp2 t2 on t1.ID= t2.ID
	where t1.SoLanLap > 0 and t1.TrangThai = 1 ---- trangthai = 1. Dang xu ly <=> chưa hẹn gặp khách
	and t2.ID is null
   
    declare @ID uniqueidentifier, @Ma_TieuDe nvarchar(max), @ID_DonVi uniqueidentifier, @ID_KhachHang uniqueidentifier,@ID_LoaiTuVan uniqueidentifier, 
    		@ID_NhanVien uniqueidentifier,@ID_NhanVienQuanLy uniqueidentifier,
    		@NgayTao datetime,@NgayGio datetime,@NgayGioKetThuc datetime, @NgayHoanThanh datetime,
    		@KieuLap int, @SoLanLap int, @GiaTriLap varchar(max), @TuanLap int, @TrangThaiKetThuc int,@GiaTriKetThuc varchar(max),			
    		@SoLanDaHen int, @TrangThai varchar, @GhiChu nvarchar(max),@NoiDung nvarchar(max),
    		@NguoiTao nvarchar(max), @MucDoUuTien int, @KetQua nvarchar(max), @NhacNho int, @KieuNhacNho int, @ID_Parent uniqueidentifier, @NgayCu datetime
    
    		--- lap ngay
    		declare _cur cursor
    		for
    			select * 
				from #tempCur t1				
				where t1.KieuLap = 1 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc, @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc, @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				-- chi add row < @ToDate
    				declare @dateadd datetime = @NgayGio

					---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate								
					if  @TrangThaiKetThuc = 1 
						begin
							declare @totalDay int = datediff(day,@dateadd, @FromDate)							
							if @totalDay > 0
								set @dateadd = DATEADD(day, @totalDay, @dateadd)
						end
				
    				declare @lanlap int = 1			
    				while @dateadd < @SToDate 
    					begin	
    						declare @dateadd_yyyyMMdd datetime= format(@dateadd,'yyyy-MM-dd')
    						if @TrangThaiKetThuc= 1 
    							OR (@TrangThaiKetThuc = 2 and  @dateadd < @GiaTriKetThuc )  --- khong bao gio OR KetThuc vao ngay OR sau x lan (todo)
    							OR (@TrangThaiKetThuc= 3 and @lanlap <= @GiaTriKetThuc - @SoLanDaHen)
    							begin
    								set @NgayGioKetThuc = DATEADD(hour,1,@dateadd)
    								declare @newidDay uniqueidentifier = NEWID()
    								declare @count1 int = 0;
    								if @dateadd = @NgayGio set @newidDay = @ID		
    								select @count1 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    									and NgayCu_yyyyMMdd = @dateadd_yyyyMMdd								
    								if @count1 = 0		
										begin
    										insert into @tblCalendar values (@newidDay,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    										@NgayTao, @dateadd,@NgayGioKetThuc, @NgayHoanThanh, @TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,
    										@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc, IIF(@dateadd = @NgayGio,'1','0'),@ID_Parent, @NgayCu)																			
											set @lanlap= @lanlap + 1
										end
    							end
    						set @dateadd = DATEADD(day, @SoLanLap, @dateadd)
    					end
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap,@TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,@ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    		--- lap tuan
    		declare _cur2 cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 2 
    		open _cur2
    		fetch next from _cur2
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin	
    				declare @weekRepeat datetime = @NgayGio			
					declare @hour int= datepart(hour, @NgayGio)
					declare @minute int= datepart(minute, @NgayGio)
    				declare @lanlapWeek int = 1

					---- tính số tuần bắt đầu từ @NgayGio in DB - đến @FromDate			
					if  @TrangThaiKetThuc = 1 -- khong bao gio ket thuc
						begin							
							declare @totalWeek int = datediff(week,@weekRepeat, @FromDate)
							if @totalWeek > 0
								set @weekRepeat = DATEADD(WEEK, @totalWeek, @weekRepeat)
						end

    				while @weekRepeat < @SToDate -- lặp đến khi thuộc khoảng thời gian tìm kiếm
    					begin	
    								declare @firstOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 0)) -- lay ngay dau tien cua tuan
    								declare @lastOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 7)) -- lay ngay cuoi cung cua tuan
    								declare @dateRepeat datetime = @firstOfWeek	
									

    								while @dateRepeat < @lastOfWeek -- tim kiem trong tuan duoc lap lai
    									begin
    										if dateadd(hour,23, @dateRepeat) >= @NgayGio
    											begin
    												declare @dateOfWeek varchar(1) = cast(DATEPART(WEEKDAY,@dateRepeat) as varchar(1)) -- lấy ngày trong tuần (thứ 2,3,..)	
    												if @dateOfWeek = 1 set @dateOfWeek = 8
    												declare @datefrom datetime = dateadd(minute, @minute, dateadd(hour,@hour, @dateRepeat))
    												set @NgayGioKetThuc = DATEADD(hour,1,@datefrom) -- add 2 hour
													declare @dateRepeat_yyyyMMdd datetime= format(@dateRepeat,'yyyy-MM-dd')
    
    												if CHARINDEX(@dateOfWeek, @GiaTriLap ) > 0 
    												and  (@TrangThaiKetThuc= 1 OR (@TrangThaiKetThuc = 2 and  @dateRepeat <= @GiaTriKetThuc)
    													OR (@TrangThaiKetThuc= 3 and @lanlapWeek <= @GiaTriKetThuc - @SoLanDaHen))
    													begin														
    														declare @newidWeek uniqueidentifier = NEWID()
    														declare @exitDB bit='0'
    														if @dateRepeat_yyyyMMdd = format(@NgayGio,'yyyy-MM-dd') 
    															begin
    																set @newidWeek = @ID
    																set @exitDB ='1'
    															end
    														declare @count2 int=0
    														select @count2 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    																and NgayCu_yyyyMMdd = @dateRepeat_yyyyMMdd								
    														if @count2 = 0	
    															begin
    																insert into @tblCalendar values (@newidWeek,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																		@NgayTao, @datefrom,@NgayGioKetThuc,  @NgayHoanThanh, 
    																		@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,
    																		@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,@exitDB, @ID_Parent, @NgayCu)
    																set @lanlapWeek= @lanlapWeek + 1																
    															end
    													end												
    											end										
    										set @dateRepeat = DATEADD(day, 1, @dateRepeat)											
    									end							
    						set @weekRepeat = DATEADD(WEEK, @SoLanLap, @weekRepeat)	-- lap lai x tuan/lan	
    					end			
    				FETCH NEXT FROM _cur2 into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur2;
    		deallocate _cur2;
    
    		--- lap thang
    		declare _cur cursor
    		for				
				select * 
				from #tempCur t1				
				where t1.KieuLap = 3 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @monthRepeat datetime = @NgayGio	
    				declare @lanlapMonth int = 1

					if  @TrangThaiKetThuc = 1
						begin
							---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate
							declare @totalMonth int = datediff(MONTH,@monthRepeat, @FromDate)
							if @totalMonth > 0
								set @monthRepeat = DATEADD(MONTH, @totalMonth, @monthRepeat)
						end

    				while @monthRepeat < @SToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin	
    						if  @monthRepeat > @FromDate			
    							begin	
    								declare @datefromMonth datetime= @monthRepeat
									declare @monthRepeat_yyyyMMdd datetime= format(@monthRepeat,'yyyy-MM-dd')

    								set @NgayGioKetThuc = DATEADD(hour,1,@datefromMonth)
    								 -- hàng tháng vào ngày ..xx..
    								if	@TuanLap = 0 
    									and (@TrangThaiKetThuc = 1 
    									OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    									OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    									)
    									begin
    											declare @newidMonth1 uniqueidentifier = NEWID()
    											if @monthRepeat = @NgayGio set @newidMonth1 = @ID
    											declare @count3 int=0
    											select @count3 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    													and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd								
    											if @count3 = 0	
    											insert into @tblCalendar values (@newidMonth1,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																@NgayTao, @monthRepeat,@NgayGioKetThuc,  @NgayHoanThanh,
    																@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    																@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc, IIF(@monthRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    									end 
    								else
    									-- hàng tháng vào thứ ..x.. tuần thứ ..y.. của tháng
    									begin
    										declare @dateOfWeek_Month int = DATEPART(WEEKDAY,@monthRepeat) -- thu may trong tuan
    										if @dateOfWeek_Month = 8 set @dateOfWeek_Month = 1
    										declare @weekOfMonth int = DATEDIFF(WEEK, DATEADD(MONTH, DATEDIFF(MONTH, 0, @monthRepeat), 0), @monthRepeat) +1 -- tuan thu may cua thang
    										if @dateOfWeek_Month = @GiaTriLap and @weekOfMonth = @TuanLap 
    										and (@TrangThaiKetThuc = 1 
    											OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    											OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    											)
    											begin
    												declare @newidMonth2 uniqueidentifier = NEWID()
    												if @monthRepeat = @NgayGio set @newidMonth2 = @ID
    												declare @count4 int=0
    												select @count4 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    														and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd							
    												if @count4 = 0	
    												insert into @tblCalendar values (@newidMonth2,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    																@NgayTao, @monthRepeat,@NgayGioKetThuc,  @NgayHoanThanh,
    																@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    																@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,IIF(@monthRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    											end
    									end						
    							end
    						set @monthRepeat = DATEADD(MONTH, @SoLanLap, @monthRepeat)	-- lap lai x thang/lan	
    						set @lanlapMonth = @lanlapMonth +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    
    		--- lap nam
    		declare _cur cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 4 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,@NoiDung,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @yearRepeat datetime = @NgayGio
					declare @yearRepeat_yyyyMMdd datetime= format(@yearRepeat,'yyyy-MM-dd')
    				declare @lanlapYear int = 1
    				while @yearRepeat < @SToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin						
    						if  @yearRepeat > @FromDate			
    							begin	
    								declare @dateOfMonth int = datepart(day,@yearRepeat)
    								declare @monthOfYear int = datepart(MONTH,@yearRepeat)
    								set @NgayGioKetThuc= DATEADD(hour,1, @yearRepeat)
    
    								if @dateOfMonth = @GiaTriLap and @monthOfYear= @TuanLap
    									and (@TrangThaiKetThuc = 1 
    										OR (@TrangThaiKetThuc = 2 and @yearRepeat < @GiaTriKetThuc)
    										OR (@TrangThaiKetThuc = 3 and @lanlapYear <= @GiaTriKetThuc - @SoLanDaHen)
    										)
    									begin
    										declare @newidYear uniqueidentifier = NEWID()										
    										if @yearRepeat = @NgayGio set @newidYear = @ID
    										declare @count5 int=0
    										select @count5 = count(ID) from #temp2 where ID_Parent = @ID_Parent 
    												and NgayCu_yyyyMMdd = @yearRepeat_yyyyMMdd							
    										if @count5 = 0	
    										insert into @tblCalendar values (@newidYear,@Ma_TieuDe, @ID_DonVi,@ID_KhachHang, @ID_LoaiTuVan, @ID_NhanVien, @ID_NhanVienQuanLy, 
    															@NgayTao, @yearRepeat,@NgayGioKetThuc, @NgayHoanThanh, 
    															@TrangThai,@GhiChu,@NoiDung,@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, 
    															@KieuLap, @SoLanLap, @GiaTriLap, @TuanLap, @TrangThaiKetThuc ,@GiaTriKetThuc,IIF(@yearRepeat = @NgayGio,'1','0'), @ID_Parent, @NgayCu)
    									end
    							end
    						set @yearRepeat = DATEADD(YEAR, @SoLanLap, @yearRepeat)	-- lap lai x nam/lan	
    						set @lanlapYear = @lanlapYear +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,@NoiDung,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;

			--select * from #temp
    	
    	-- add LichHen da duoc update (SoLanLap = 0)
    	insert into @tblCalendar
    	select tbl1.ID, 
    		Ma_TieuDe,
    		ID_DonVi, 
    		ID_KhachHang, 
    		ID_LoaiTuVan,
    		ID_NhanVien,	
    		ID_NhanVienQuanLy,
    		NgayTao,
    		NgayGio,
    		NgayGioKetThuc,
    		NgayHoanThanh,
    		TrangThai,
    		GhiChu,
			NoiDung,
    		NguoiTao,
    		2 as MucDoUuTien, 
    		KetQua,
    		NhacNho,
    		ISNULL(KieuNhacNho,0) as KieuNhacNho,
    		ISNULL(KieuLap,0) as KieuLap,
    		ISNULL(SoLanLap,0) as SoLanLap,
    		ISNULL(GiaTriLap,'') as GiaTriLap,
    		ISNULL(TuanLap,0) as TuanLap,
    		ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    		ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc,
    		'1' as ExistDB, 
    		tbl1.ID_Parent,
    		tbl1.NgayCu
    	from #temp tbl1
		left join #temp2 tbl2 on tbl1.ID = tbl2.ID
		where (KieuLap = 0 OR TrangThai !='1' 
    		Or tbl2.ID is not null
    		)
    		and NgayGio between @FromDate and @SToDate;
    			
    	--drop table #temp 
    	--drop table #temp2 ;

		with data_cte
		as 
		(    
    	select b.*, 
			case when PhanLoai= 3 then 'rgb(11, 128, 67)' -- lichhen: xanhla
				else
					case when MucDoUuTien= 1 then '#ff6b77' else 'rgb(3, 155, 229)' -- uutiencao: hongnhat, nguoclai: congviec xanhtroi
			end end as color,
			TenNhanVien, 
    		ISNULL(LoaiDoiTuong,0) as LoaiDoiTuong, 
    		ISNULL(MaDoiTuong,'') as MaDoiTuong, 
    		ISNULL(TenDoiTuong,'') as TenDoiTuong, 
    		ISNULL(DienThoai,'') as DienThoai, 
			ISNULL(TenNhomDoiTuongs,N'Nhóm mặc định') as TenNhomDoiTuongs,    		
			dt.ID_NguonKhach,
    		ISNULL(TenLoaiTuVanLichHen,'') as TenLoaiTuVanLichHen, ISNULL(nk.TenNguonKhach,'') as TenNguonKhach,
			case when dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' else ISNULL(dt.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as IDNhomDoiTuongs,
			case when NgayBatDau > @today then 0 else 1 end as DateSort
			
    	from
    		(select *, format(NgayGio, 'yyyy-MM-dd') as NgayBatDau
    		from
    			(-- lichhen
    			select ID,
    				Ma_TieuDe,
    				ID_DonVi, 
    				ID_KhachHang, 
    				ISNULL(ID_LoaiTuVan,'00000000-0000-0000-0000-000000000000') as ID_LoaiTuVan,
    				ID_NhanVien,
    				ISNULL(ID_NhanVienQuanLy,'00000000-0000-0000-0000-000000000000') as ID_NhanVienQuanLy,
    				NgayTao,
    				NgayHenGap as NgayGio,
    				NgayGioKetThuc,
    				NgayHoanThanh,
    				TrangThai,
    				GhiChu,
					NoiDung,
    				NguoiTao,
    				MucDoUuTien,
    				KetQua,
    				NhacNho,			
    				KieuNhacNho,
    				KieuLap,
    				SoLanLap, 
    				GiaTriLap, 
    				TuanLap, 
    				TrangThaiKetThuc ,
    				GiaTriKetThuc,
    				3 as PhanLoai,
    				'0' as CaNgay,
    				ExistDB, 
    				ID_Parent,
    				NgayCu
    			from @tblCalendar
    			where exists (select ID from @tblTrangThai tt where tt.TrangThaiCV = TrangThai) 
    			and NgayHenGap between @FromDate and @SToDate
    
    			union all
    			-- cong viec
    			select 
    					cs.ID,
    					Ma_TieuDe,
    					cs.ID_DonVi, 
    					ID_KhachHang, 
    					ISNULL(ID_LoaiTuVan,'00000000-0000-0000-0000-000000000000') as ID_LoaiTuVan,
    					ID_NhanVien,
    					ISNULL(ID_NhanVienQuanLy,'00000000-0000-0000-0000-000000000000') as ID_NhanVienQuanLy,
    					cs.NgayTao,
    					NgayGio,
    					NgayGioKetThuc,
    					NgayHoanThanh,
    					cs.TrangThai,
    					cs.GhiChu,
						cs.NoiDung,
    					cs.NguoiTao,
    					MucDoUuTien,
    					KetQua,
    					cs.NhacNho,
    					ISNULL(KieuNhacNho,0) as KieuNhacNho,
    					ISNULL(KieuLap,0) as KieuLap,
    					ISNULL(SoLanLap,0) as SoLanLap,
    					ISNULL(GiaTriLap,'') as GiaTriLap,
    					ISNULL(TuanLap,0) as TuanLap,
    					ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    					ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc,
    					4 as PhanLoai,
    					ISNULL(cs.CaNgay,'0') as CaNgay,
    					'1' as ExistDB,
    					ID_Parent,
    					NgayCu
    				from ChamSocKhachHangs cs
    				where PhanLoai= 4
    				and exists (select ID from @tblTrangThai tt where tt.TrangThaiCV = TrangThai) 
    				and NgayGio between  @FromDate and  @SToDate
    				) a
    			where 
				exists (select ID from @tblNVPhuTrach nvpt where nvpt.ID= a.ID_NhanVien)
    			and 
				exists (select ID from @tblLoaiCV loaiCV where loaiCV.ID = a.ID_LoaiTuVan)
				and exists (select ID from @tblChiNhanh cn where cn.id = a.ID_DonVi)
    		)b
    		left join DM_DoiTuong dt on b.ID_KhachHang= dt.ID
    		left join NS_NhanVien nv on b.ID_NhanVien= nv.ID   	
			left join DM_LoaiTuVanLichHen loai on b.ID_LoaiTuVan= loai.ID
    		left join DM_NguonKhachHang nk on dt.ID_NguonKhach= nk.ID
    		where (dt.LoaiDoiTuong like @LoaiDoiTuong OR LoaiDoiTuong is null)
    		and
			b.PhanLoai like @PhanLoai
    		and b.MucDoUuTien like @DoUuTien
    		and ISNULL(dt.ID,'00000000-0000-0000-0000-000000000000') like @IDKhachHang
			AND
		((select count(Name) from @tblSearchString tblS where 
				b.Ma_TieuDe like '%'+tblS.Name+'%' 
    			or b.GhiChu like N'%'+tblS.Name+'%'
				or dt.TenDoiTuong like '%'+tblS.Name+'%'
    			or dt.TenDoiTuong_KhongDau  like '%'+tblS.Name+'%'
				or dt.MaDoiTuong like '%'+tblS.Name+'%'
    			or dt.DienThoai  like '%'+tblS.Name+'%'
				or nv.MaNhanVien like N'%'+tblS.Name+'%'
				or nv.TenNhanVien like N'%'+tblS.Name+'%'					
    			or nv.TenNhanVienKhongDau like '%'+tblS.Name+'%')=@count or @count=0)    
				
			),
			count_cte
			as
			(
			select count (ID) as TotalRow,
				CEILING(count(ID)/CAST(@PageSize as float )) as TotalPage
			from data_cte
			)
			select dt.*, cte.*
			from data_cte dt
			cross join count_cte cte
			order by dt.DateSort, dt.NgayGio
		
			
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListLichHen_FullCalendar_Dashboard]
    @IDChiNhanhs [nvarchar](max),
    @PhanLoai [nvarchar](20),
	@FromDate datetime
AS
BEGIN
    SET NOCOUNT ON;
		
		declare @SFromDate nvarchar(14) =  convert(varchar(14), DATEADD(SECOND, -2, @FromDate),23) --- used to sosanh nhung lich  From
    	declare @ToDate datetime= convert(varchar(14), dateadd(DAY,1,@FromDate),23) --- used to sosanh nhung lich  To
        
    	declare @tblCalendar table(ID uniqueidentifier, Ma_TieuDe nvarchar(max), ID_DonVi uniqueidentifier, ID_NhanVien uniqueidentifier, NgayHenGap datetime, TrangThai varchar(10))
    	
    
    --- table LichHen
    select cs.ID, 
    	Ma_TieuDe,
    	cs.ID_DonVi, 
    	ID_KhachHang, 
    	ID_LoaiTuVan,
    	ID_NhanVien,	
    	ID_NhanVienQuanLy,
    	NgayTao,
    	NgayGio,
    	NgayGioKetThuc,	
    	NgayHoanThanh,
    	ISNULL(KieuLap,0) as KieuLap,
    	ISNULL(SoLanLap,0) as SoLanLap,
    	ISNULL(GiaTriLap,'') as GiaTriLap,
    	ISNULL(TuanLap,0) as TuanLap,
    	ISNULL(TrangThaiKetThuc,0) as TrangThaiKetThuc,
    	ISNULL(GiaTriKetThuc,'') as GiaTriKetThuc, 	
    	ISNULL(SoLanDaHen,0) as SoLanDaHen,
    	TrangThai,
    	ISNULL(GhiChu,'') as GhiChu,
    	NguoiTao,
    	2 as MucDoUuTien,
    	KetQua,
    	NhacNho, 
    	ISNULL(KieuNhacNho,0) as KieuNhacNho,
		case when cs.ID_Parent is null then cs.ID else cs.ID_Parent end as ID_Parent,
    	cs.NgayCu
    into #calendar_temp1
    from ChamSocKhachHangs cs
    left join ( select ISNULL(ID_Parent,'00000000-0000-0000-0000-000000000000') as ID_Parent,
    		count(*) as SoLanDaHen
    		from ChamSocKhachHangs
    		where PhanLoai = 3
    		group by ID_Parent) a on cs.ID= a.ID_Parent
    where KieuLap is not null
    	and (TrangThaiKetThuc = 1 
    	OR (TrangThaiKetThuc = 2 and ISNULL(GiaTriKetThuc,'')  > @SFromDate)
    	OR (TrangThaiKetThuc = 3 and ISNULL(SoLanDaHen,0)  <= ISNULL(GiaTriKetThuc,0)) 
    	)	
    and PhanLoai = 3 
	and cs.ID_DonVi =@IDChiNhanhs
    
	 -- get row was update (ID_Parent !=null)
    select ID, ID_Parent, NgayCu, format(NgayCu,'yyyy-MM-dd') as NgayCu_yyyyMMdd 
	into #calendar_temp2  
	from #calendar_temp1 where ID_Parent != ID


	
	select t1.* 
	into #tempCur 
	from #calendar_temp1 t1
	left join #calendar_temp2 t2 on t1.ID= t2.ID
	where t1.SoLanLap > 0 and t1.TrangThai = 1 ---- trangthai = 1. Dang xu ly <=> chưa hẹn gặp khách
	and t2.ID is null
    
    
    declare @ID uniqueidentifier, @Ma_TieuDe nvarchar(max), @ID_DonVi uniqueidentifier, @ID_KhachHang uniqueidentifier,@ID_LoaiTuVan uniqueidentifier, 
    		@ID_NhanVien uniqueidentifier,@ID_NhanVienQuanLy uniqueidentifier,
    		@NgayTao datetime,@NgayGio datetime,@NgayGioKetThuc datetime, @NgayHoanThanh datetime,
    		@KieuLap int, @SoLanLap int, @GiaTriLap varchar(max), @TuanLap int, @TrangThaiKetThuc int,@GiaTriKetThuc varchar(max),			
    		@SoLanDaHen int, @TrangThai varchar, @GhiChu nvarchar(max),
    		@NguoiTao nvarchar(max), @MucDoUuTien int, @KetQua nvarchar(max), @NhacNho int, @KieuNhacNho int, @ID_Parent uniqueidentifier, @NgayCu datetime
    
    		--- lap ngay
    		declare _cur cursor
    		for   	
				select * 
				from #tempCur t1				
				where t1.KieuLap = 1 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc, @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc, @SoLanDaHen,@TrangThai,@GhiChu,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				-- chi add row < @ToDate
    				declare @dateadd datetime = @NgayGio

					---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate								
					if  @TrangThaiKetThuc = 1 
						begin
							declare @totalDay int = datediff(day,@dateadd, @FromDate)							
							if @totalDay > 0
								set @dateadd = DATEADD(day, @totalDay, @dateadd)
						end
				
    				declare @lanlap int = 1			
    				while @dateadd < @ToDate 
    					begin	
    						declare @dateadd_yyyyMMdd datetime= format(@dateadd,'yyyy-MM-dd')
    						if @TrangThaiKetThuc= 1 
    							OR (@TrangThaiKetThuc = 2 and  @dateadd < @GiaTriKetThuc )  --- khong bao gio OR KetThuc vao ngay OR sau x lan (todo)
    							OR (@TrangThaiKetThuc= 3 and @lanlap <= @GiaTriKetThuc - @SoLanDaHen)
    							begin
    								set @NgayGioKetThuc = DATEADD(hour,1,@dateadd)
    								declare @newidDay uniqueidentifier = NEWID()
    								declare @count1 int = 0;
    								if @dateadd = @NgayGio set @newidDay = @ID		
    								select @count1 = count(ID) from #calendar_temp2 where ID_Parent = @ID_Parent 
    									and NgayCu_yyyyMMdd = @dateadd_yyyyMMdd								
    								if @count1 = 0		
										begin
    										insert into @tblCalendar values (@newidDay,@Ma_TieuDe, @ID_DonVi, @ID_NhanVien, @dateadd, @TrangThai)																											
											set @lanlap= @lanlap + 1
										end
    							end
    						set @dateadd = DATEADD(day, @SoLanLap, @dateadd)
    					end
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap,@TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho,@ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    		--- lap tuan
    		declare _cur2 cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 2 
    		open _cur2
    		fetch next from _cur2
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin	
    				declare @weekRepeat datetime = @NgayGio			
					declare @hour int= datepart(hour, @NgayGio)
					declare @minute int= datepart(minute, @NgayGio)
    				declare @lanlapWeek int = 1

					---- tính số tuần bắt đầu từ @NgayGio in DB - đến @FromDate			
					if  @TrangThaiKetThuc = 1 -- khong bao gio ket thuc
						begin							
							declare @totalWeek int = datediff(week,@weekRepeat, @FromDate)
							if @totalWeek > 0
								set @weekRepeat = DATEADD(WEEK, @totalWeek, @weekRepeat)
						end

    				while @weekRepeat < @ToDate -- lặp đến khi thuộc khoảng thời gian tìm kiếm
    					begin	
    								declare @firstOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 0)) -- lay ngay dau tien cua tuan
    								declare @lastOfWeek datetime = (select  dateadd(WEEK, datediff(WEEK, 0, @weekRepeat), 7)) -- lay ngay cuoi cung cua tuan
    								declare @dateRepeat datetime = @firstOfWeek	
									

    								while @dateRepeat < @lastOfWeek -- tim kiem trong tuan duoc lap lai
    									begin
    										if dateadd(hour,23, @dateRepeat) >= @NgayGio
    											begin
    												declare @dateOfWeek varchar(1) = cast(DATEPART(WEEKDAY,@dateRepeat) as varchar(1)) -- lấy ngày trong tuần (thứ 2,3,..)	
    												if @dateOfWeek = 1 set @dateOfWeek = 8
    												declare @datefrom datetime = dateadd(minute, @minute, dateadd(hour,@hour, @dateRepeat))
    												set @NgayGioKetThuc = DATEADD(hour,1,@datefrom) -- add 2 hour
													declare @dateRepeat_yyyyMMdd datetime= format(@dateRepeat,'yyyy-MM-dd')
    
    												if CHARINDEX(@dateOfWeek, @GiaTriLap ) > 0 
    												and  (@TrangThaiKetThuc= 1 OR (@TrangThaiKetThuc = 2 and  @dateRepeat <= @GiaTriKetThuc)
    													OR (@TrangThaiKetThuc= 3 and @lanlapWeek <= @GiaTriKetThuc - @SoLanDaHen))
    													begin														
    														declare @newidWeek uniqueidentifier = NEWID()
    														declare @exitDB bit='0'
    														if @dateRepeat_yyyyMMdd = format(@NgayGio,'yyyy-MM-dd') 
    															begin
    																set @newidWeek = @ID
    																set @exitDB ='1'
    															end
    														declare @count2 int=0
    														select @count2 = count(ID) from #calendar_temp2 where ID_Parent = @ID_Parent 
    																and NgayCu_yyyyMMdd = @dateRepeat_yyyyMMdd								
    														if @count2 = 0	
    															begin
    																insert into @tblCalendar values (@newidWeek, @Ma_TieuDe, @ID_DonVi, @ID_NhanVien, @dateRepeat, @TrangThai)	
    																set @lanlapWeek= @lanlapWeek + 1																
    															end
    													end												
    											end										
    										set @dateRepeat = DATEADD(day, 1, @dateRepeat)											
    									end							
    						set @weekRepeat = DATEADD(WEEK, @SoLanLap, @weekRepeat)	-- lap lai x tuan/lan	
    					end			
    				FETCH NEXT FROM _cur2 into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur2;
    		deallocate _cur2;
    
    		--- lap thang
    		declare _cur cursor
    		for				
				select * 
				from #tempCur t1				
				where t1.KieuLap = 3 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @monthRepeat datetime = @NgayGio	
    				declare @lanlapMonth int = 1

					if  @TrangThaiKetThuc = 1
						begin
							---- tính số ngày bắt đầu từ @NgayGio in DB - đến @FromDate
							declare @totalMonth int = datediff(MONTH,@monthRepeat, @FromDate)
							if @totalMonth > 0
								set @monthRepeat = DATEADD(MONTH, @totalMonth, @monthRepeat)
						end

    				while @monthRepeat < @ToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin	
    						if  @monthRepeat > @FromDate			
    							begin	
    								declare @datefromMonth datetime= @monthRepeat
									declare @monthRepeat_yyyyMMdd datetime= format(@monthRepeat,'yyyy-MM-dd')

    								set @NgayGioKetThuc = DATEADD(hour,1,@datefromMonth)
    								 -- hàng tháng vào ngày ..xx..
    								if	@TuanLap = 0 
    									and (@TrangThaiKetThuc = 1 
    									OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    									OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    									)
    									begin
    											declare @newidMonth1 uniqueidentifier = NEWID()
    											if @monthRepeat = @NgayGio set @newidMonth1 = @ID
    											declare @count3 int=0
    											select @count3 = count(ID) from #calendar_temp2 where ID_Parent = @ID_Parent 
    													and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd								
    											if @count3 = 0	
    												insert into @tblCalendar values (@newidMonth1, @Ma_TieuDe, @ID_DonVi, @ID_NhanVien, @monthRepeat, @TrangThai)	
    									end 
    								else
    									-- hàng tháng vào thứ ..x.. tuần thứ ..y.. của tháng
    									begin
    										declare @dateOfWeek_Month int = DATEPART(WEEKDAY,@monthRepeat) -- thu may trong tuan
    										if @dateOfWeek_Month = 8 set @dateOfWeek_Month = 1
    										declare @weekOfMonth int = DATEDIFF(WEEK, DATEADD(MONTH, DATEDIFF(MONTH, 0, @monthRepeat), 0), @monthRepeat) +1 -- tuan thu may cua thang
    										if @dateOfWeek_Month = @GiaTriLap and @weekOfMonth = @TuanLap 
    										and (@TrangThaiKetThuc = 1 
    											OR (@TrangThaiKetThuc = 2 and @monthRepeat < @GiaTriKetThuc)
    											OR (@TrangThaiKetThuc= 3 and @lanlapMonth <= @GiaTriKetThuc - @SoLanDaHen)
    											)
    											begin
    												declare @newidMonth2 uniqueidentifier = NEWID()
    												if @monthRepeat = @NgayGio set @newidMonth2 = @ID
    												declare @count4 int=0
    												select @count4 = count(ID) from #calendar_temp2 where ID_Parent = @ID_Parent 
    														and NgayCu_yyyyMMdd = @monthRepeat_yyyyMMdd							
    												if @count4 = 0	
    													insert into @tblCalendar values (@newidMonth2, @Ma_TieuDe, @ID_DonVi, @ID_NhanVien, @monthRepeat, @TrangThai)
    											end
    									end						
    							end
    						set @monthRepeat = DATEADD(MONTH, @SoLanLap, @monthRepeat)	-- lap lai x thang/lan	
    						set @lanlapMonth = @lanlapMonth +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    
    			--- lap nam
    		declare _cur cursor
    		for
				select * 
				from #tempCur t1				
				where t1.KieuLap = 4 
    		open _cur
    		fetch next from _cur
    		into @ID, @Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan, @ID_NhanVien,@ID_NhanVienQuanLy,
    			@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    			@KieuLap, @SoLanLap, @GiaTriLap,@TuanLap, @TrangThaiKetThuc,@GiaTriKetThuc,  @SoLanDaHen,@TrangThai,@GhiChu,
    			@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    		while @@FETCH_STATUS = 0
    			begin		
    				declare @yearRepeat datetime = @NgayGio
					declare @yearRepeat_yyyyMMdd datetime= format(@yearRepeat,'yyyy-MM-dd')
    				declare @lanlapYear int = 1
    				while @yearRepeat < @ToDate -- lặp trong khoảng thời gian tìm kiếm
    					begin						
    						if  @yearRepeat > @FromDate			
    							begin	
    								declare @dateOfMonth int = datepart(day,@yearRepeat)
    								declare @monthOfYear int = datepart(MONTH,@yearRepeat)
    								set @NgayGioKetThuc= DATEADD(hour,1, @yearRepeat)
    
    								if @dateOfMonth = @GiaTriLap and @monthOfYear= @TuanLap
    									and (@TrangThaiKetThuc = 1 
    										OR (@TrangThaiKetThuc = 2 and @yearRepeat < @GiaTriKetThuc)
    										OR (@TrangThaiKetThuc = 3 and @lanlapYear <= @GiaTriKetThuc - @SoLanDaHen)
    										)
    									begin
    										declare @newidYear uniqueidentifier = NEWID()										
    										if @yearRepeat = @NgayGio set @newidYear = @ID
    										declare @count5 int=0
    										select @count5 = count(ID) from #calendar_temp2 where ID_Parent = @ID_Parent 
    												and NgayCu_yyyyMMdd = @yearRepeat_yyyyMMdd							
    										if @count5 = 0	
    											insert into @tblCalendar values (@newidYear, @Ma_TieuDe, @ID_DonVi,  @ID_NhanVien,@yearRepeat, @TrangThai)
    									end
    							end
    						set @yearRepeat = DATEADD(YEAR, @SoLanLap, @yearRepeat)	-- lap lai x nam/lan	
    						set @lanlapYear = @lanlapYear +1
    					end			
    				FETCH NEXT FROM _cur into @ID,@Ma_TieuDe, @ID_DonVi, @ID_KhachHang,@ID_LoaiTuVan,@ID_NhanVien,@ID_NhanVienQuanLy,
    					@NgayTao, @NgayGio, @NgayGioKetThuc,  @NgayHoanThanh,
    					@KieuLap, @SoLanLap,@GiaTriLap, @TuanLap, @TrangThaiKetThuc, @GiaTriKetThuc, @SoLanDaHen, @TrangThai,@GhiChu,
    					@NguoiTao, @MucDoUuTien, @KetQua, @NhacNho, @KieuNhacNho, @ID_Parent, @NgayCu
    			end
    		close _cur;
    		deallocate _cur;
    	
    	-- add LichHen da duoc update (SoLanLap = 0)
    	insert into @tblCalendar
    	select tbl1.ID, 
			Ma_TieuDe,
    		tbl1.ID_DonVi, 
    		ID_NhanVien,	
    		NgayGio,
    		TrangThai
    	from #calendar_temp1 tbl1
    	left join #calendar_temp2 tbl2 on tbl1.ID = tbl2.ID
		where (KieuLap = 0 OR TrangThai !='1' 
    		Or tbl2.ID is not null
    		)
    		and NgayGio between @FromDate and @ToDate;
    	
    
    
    	-- select --> union
    	select b.*
    	from
    		(select *
    		from
    			(-- lichhen
    			select ID,				
    				ID_DonVi, 
    				ID_NhanVien,
    				NgayHenGap as NgayGio,
    				3 as PhanLoai
    			from @tblCalendar
    			where NgayHenGap between @FromDate and @ToDate and NgayHenGap != @ToDate --- between dang lay ca dinh dang gio 00:00:00 
    			and TrangThai='1'    		
    
    			union all
    			-- cong viec
    			select 
    					cs.ID,
    					cs.ID_DonVi, 
    					ID_NhanVien,
    					NgayGio,
    					4 as PhanLoai
    				from ChamSocKhachHangs cs
    				where PhanLoai= 4    				
    				and TrangThai='1'
    				and NgayGio between  @FromDate and  @ToDate
    				) a			
    		)b
    		where b.PhanLoai like @PhanLoai
    		order by NgayGio
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetMaHoaDonMax_byTemp]
	@LoaiHoaDon int,
	@ID_DonVi uniqueidentifier,
	@NgayLapHoaDon datetime
AS
BEGIN	
	
	SET NOCOUNT ON;

	DECLARE @ReturnVC NVARCHAR(MAX);
	declare @NgayLapHoaDon_Compare datetime = dateadd(month, -1, @NgayLapHoaDon)	
	declare @ngaytaoHDMax datetime
	
	declare @tblLoaiHD table (LoaiHD int)
	if @LoaiHoaDon = 4 or @LoaiHoaDon = 13 or @LoaiHoaDon = 14 ---- nhapkho noibo + nhaphangthua = dùng chung mã với nhập kho nhà cung cấp
		begin
			set @LoaiHoaDon = 4
			insert into @tblLoaiHD values (4),(13),(14)			
		end
	else 
		begin
			insert into @tblLoaiHD values (@LoaiHoaDon)	
		end
	
	
	DECLARE @Return float = 1
	declare @lenMaMax int = 0
	declare @kihieuchungtu varchar(10),  @lenMaChungTu int =0

	DECLARE @isDefault bit = (select top 1 SuDungMaChungTu from HT_CauHinhPhanMem where ID_DonVi= @ID_DonVi)-- co/khong thiet lap su dung Ma MacDinh
	DECLARE @isSetup int = (select top 1 ID_LoaiChungTu from HT_MaChungTu where ID_LoaiChungTu = @LoaiHoaDon)-- da ton tai trong bang thiet lap chua

	if @isDefault='1' and @isSetup is not null
		begin
			DECLARE @machinhanh varchar(15) = (select MaDonVi from DM_DonVi where ID= @ID_DonVi)
			
			DECLARE @isUseMaChiNhanh varchar(15), @kituphancach1 varchar(1),  @kituphancach2 varchar(1),  @kituphancach3 varchar(1),
			 @dinhdangngay varchar(8), @dodaiSTT INT

			 select @isUseMaChiNhanh = SuDungMaDonVi, 
				@kituphancach1= KiTuNganCach1,
				@kituphancach2 = KiTuNganCach2,
				@kituphancach3= KiTuNganCach3,
				@dinhdangngay = NgayThangNam,
				@dodaiSTT = CAST(DoDaiSTT AS INT),
				@kihieuchungtu = MaLoaiChungTu
			 from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon 

			
			
			DECLARE @namthangngay varchar(10) = convert(varchar(10), @NgayLapHoaDon, 112)
			DECLARE @year varchar(4) = Left(@namthangngay,4)
			DECLARE @date varchar(4) = right(@namthangngay,2)
			DECLARE @month varchar(4) = substring(@namthangngay,5,2)
			DECLARE @datecompare varchar(10)='';
			
			if	@isUseMaChiNhanh='0'
			begin 
				set @machinhanh=''
				set @kituphancach1 =''													
			end						

			if @dinhdangngay='ddMMyyyy'
				set @datecompare = CONCAT(@date,@month,@year)
			else	
				if @dinhdangngay='ddMMyy'
					set @datecompare = CONCAT(@date,@month,right(@year,2))
				else 
					if @dinhdangngay='MMyyyy'
						set @datecompare = CONCAT(@month,@year)
					else	
						if @dinhdangngay='MMyy'
							set @datecompare = CONCAT(@month,right(@year,2))
						else
							if @dinhdangngay='yyyyMMdd'
								set @datecompare = CONCAT(@year,@month,@date)
							else 
								if @dinhdangngay='yyMMdd'
									set @datecompare = CONCAT(right(@year,2),@month,@date)
								else	
									if @dinhdangngay='yyyyMM'
										set @datecompare = CONCAT(@year,@month)
									else	
										if @dinhdangngay='yyMM'
											set @datecompare = CONCAT(right(@year,2),@month)
										else 
											if @dinhdangngay='yyyy'
												set @datecompare = @year							

			DECLARE @sMaFull varchar(50) = concat(@machinhanh,@kituphancach1,@kihieuchungtu,@kituphancach2, @datecompare, @kituphancach3)	

			declare @sCompare varchar(30) = @sMaFull
			if @sMaFull= concat(@kihieuchungtu,'_') set @sCompare = concat(@kihieuchungtu,'[_]') -- like %_% không nhận kí tự _ nên phải [_] theo quy tắc của sql
			
			set @lenMaChungTu = Len(@sMaFull); ----- !!important: chỉ lấy những kí tự nằm bên phải mã full

				----- don't check ID_DonVi (because MaHoaDon like @sCompare contains (MaChiNhanh)
				select @ngaytaoHDMax = max(NgayTao)
				from BH_HoaDon hd				
				where MaHoaDon like @sCompare +'%'
				and exists (select * from @tblLoaiHD loai where hd.LoaiHoaDon= loai.LoaiHD)

				if @NgayLapHoaDon_Compare  > @ngaytaoHDMax set @NgayLapHoaDon_Compare = format(@ngaytaoHDMax,'yyyy-MM-dd')

				SET @Return = 
				(
					select  
						max(CAST(dbo.udf_GetNumeric(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu))AS float)) as MaxNumber
					from
					(
						select MaHoaDon
						from BH_HoaDon hd				
						where MaHoaDon like @sCompare +'%'
						and exists (select * from @tblLoaiHD loai where hd.LoaiHoaDon= loai.LoaiHD)
						and NgayTao > @NgayLapHoaDon_Compare									
					) a
					where ISNUMERIC(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu)) = 1
				)

				
			-- lay chuoi 000
			declare @stt int =0;
			declare @strstt varchar (10) ='0'
			while @stt < @dodaiSTT- 1
				begin
					set @strstt= CONCAT('0',@strstt)
					SET @stt = @stt +1;
				end 
			declare @lenSst int = len(@strstt)
			if	@Return is null 
					select CONCAT(@sMaFull,left(@strstt,@lenSst-1),1) as MaxCode-- left(@strstt,@lenSst-1): bỏ bớt 1 số 0			
			else 
				begin
					set @ReturnVC = FORMAT(@Return + 1, 'F0');
					set @lenMaMax = len(@ReturnVC)

					-- neu @Return là 1 số quá lớn --> mã bị chuyển về dạng e+10
					declare @madai nvarchar(max)= CONCAT(@sMaFull, CONVERT(numeric(22,0), @ReturnVC))
					select 
						case @lenMaMax							
							when 1 then CONCAT(@sMaFull,left(@strstt,@lenSst-1),@ReturnVC)
							when 2 then case when @lenSst - 2 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-2), @ReturnVC) else @madai end
							when 3 then case when @lenSst - 3 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-3), @ReturnVC) else @madai end
							when 4 then case when @lenSst - 4 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-4), @ReturnVC) else @madai end
							when 5 then case when @lenSst - 5 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-5), @ReturnVC) else @madai end
							when 6 then case when @lenSst - 6 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-6), @ReturnVC) else @madai end
							when 7 then case when @lenSst - 7 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-7), @ReturnVC) else @madai end
							when 8 then case when @lenSst - 8 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-8), @ReturnVC) else @madai end
							when 9 then case when @lenSst - 9 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-9), @ReturnVC) else @madai end
							when 10 then case when @lenSst - 10 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-10), @ReturnVC) else @madai end
						else 
							case when  @lenMaMax > 10
								 then iif(@lenSst - 10 > -1, CONCAT(@sMaFull, left(@strstt,@lenSst-10), @ReturnVC),  @madai)
								 else '' end
						end as MaxCode					
				end 
		end
	else
		begin		
			set @kihieuchungtu = (select top 1 MaLoaiChungTu from DM_LoaiChungTu where ID= @LoaiHoaDon)
			set @lenMaChungTu  = LEN(@kihieuchungtu)
						
			IF @LoaiHoaDon = 30
			BEGIN

				----- phieu bangiaoxe (not use)
				DECLARE @MaHoaDonMax NVARCHAR(MAX);
				
				select TOP 1 @MaHoaDonMax = MaPhieu
				from Gara_Xe_PhieuBanGiao 
				where SUBSTRING(MaPhieu, 1, len(@kihieuchungtu)) = @kihieuchungtu 
				and CHARINDEX('O',MaPhieu) = 0 
				AND LEN(MaPhieu) = 10 + @lenMaChungTu 
				AND ISNUMERIC(RIGHT(MaPhieu,LEN(MaPhieu)- @lenMaChungTu)) = 1
				ORDER BY MaPhieu DESC;

				SET @Return = CAST(dbo.udf_GetNumeric(RIGHT(@MaHoaDonMax,LEN(@MaHoaDonMax)- @lenMaChungTu))AS float);
			END
			ELSE
			BEGIN
				select @ngaytaoHDMax = max(NgayTao)
				from BH_HoaDon hd				
				where MaHoaDon like @kihieuchungtu +'%'
				and exists (select * from @tblLoaiHD loai where hd.LoaiHoaDon= loai.LoaiHD)

				if @NgayLapHoaDon_Compare  > @ngaytaoHDMax set @NgayLapHoaDon_Compare = format(@ngaytaoHDMax,'yyyy-MM-dd')			

				SET @Return = 
				(
					select  
						max(CAST(dbo.udf_GetNumeric(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu))AS float)) as MaxNumber
					from
					(
						select MaHoaDon
						from dbo.BH_HoaDon	hd						
						where MaHoaDon like @kihieuchungtu +'%'
						and NgayTao > @NgayLapHoaDon_Compare
						and exists (select * from @tblLoaiHD loai where hd.LoaiHoaDon= loai.LoaiHD)
					) a
					where ISNUMERIC(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu)) = 1
				)
				
			END
		
			-- do dai STT (toida = 10)
			if	@Return is null 
					select
						case when @lenMaChungTu = 2 then CONCAT(@kihieuchungtu, '00000000',1)
							when @lenMaChungTu = 3 then CONCAT(@kihieuchungtu, '0000000',1)
							when @lenMaChungTu = 4 then CONCAT(@kihieuchungtu, '000000',1)
							when @lenMaChungTu = 5 then CONCAT(@kihieuchungtu, '00000',1)
						else CONCAT(@kihieuchungtu,'000000',1)
						end as MaxCode
			else 
				begin
					set @ReturnVC = FORMAT(@Return + 1, 'F0');
					set @lenMaMax = len(@ReturnVC)
					select 
						case @lenMaMax
							when 1 then CONCAT(@kihieuchungtu,'000000000',@ReturnVC)
							when 2 then CONCAT(@kihieuchungtu,'00000000',@ReturnVC)
							when 3 then CONCAT(@kihieuchungtu,'0000000',@ReturnVC)
							when 4 then CONCAT(@kihieuchungtu,'000000',@ReturnVC)
							when 5 then CONCAT(@kihieuchungtu,'00000',@ReturnVC)
							when 6 then CONCAT(@kihieuchungtu,'0000',@ReturnVC)
							when 7 then CONCAT(@kihieuchungtu,'000',@ReturnVC)
							when 8 then CONCAT(@kihieuchungtu,'00',@ReturnVC)
							when 9 then CONCAT(@kihieuchungtu,'0',@ReturnVC)								
						else CONCAT(@kihieuchungtu,@ReturnVC) end as MaxCode						
				end 
		end
		
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetMaPhieuThuChiMax_byTemp]
    @LoaiHoaDon [int],
    @ID_DonVi [uniqueidentifier],
    @NgayLapHoaDon [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	DECLARE @Return float = 1
    	declare @lenMaMax int = 0
    	DECLARE @isDefault bit = (select top 1 SuDungMaChungTu from HT_CauHinhPhanMem where ID_DonVi= @ID_DonVi)-- co/khong thiet lap su dung Ma MacDinh
    	DECLARE @isSetup int = (select top 1 ID_LoaiChungTu from HT_MaChungTu where ID_LoaiChungTu = @LoaiHoaDon)
    
    	if @isDefault='1' and @isSetup is not null
    		begin
    			DECLARE @machinhanh varchar(15) = (select MaDonVi from DM_DonVi where ID= @ID_DonVi)
    			DECLARE @lenMaCN int = Len(@machinhanh)
    			DECLARE @isUseMaChiNhanh varchar(15) = (select SuDungMaDonVi from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon) -- co/khong su dung MaChiNhanh
    			DECLARE @kituphancach1 varchar(1) = (select KiTuNganCach1 from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @kituphancach2 varchar(1) = (select KiTuNganCach2 from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @kituphancach3 varchar(1) = (select KiTuNganCach3 from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @dinhdangngay varchar(8) = (select NgayThangNam from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @dodaiSTT INT = (select CAST(DoDaiSTT AS INT) from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @kihieuchungtu varchar(10) = (select MaLoaiChungTu from HT_MaChungTu where ID_LoaiChungTu=@LoaiHoaDon)
    			DECLARE @lenMaKiHieu int = Len(@kihieuchungtu);
    			DECLARE @namthangngay varchar(10) = convert(varchar(10), @NgayLapHoaDon, 112)
    			DECLARE @year varchar(4) = Left(@namthangngay,4)
    			DECLARE @date varchar(4) = right(@namthangngay,2)
    			DECLARE @month varchar(4) = substring(@namthangngay,5,2)
    			DECLARE @datecompare varchar(10)='';
    			
    			if	@isUseMaChiNhanh='0'
    				begin 
    					set @machinhanh=''
    					set @lenMaCN=0
    				end
    
    			if @dinhdangngay='ddMMyyyy'
    				set @datecompare = CONCAT(@date,@month,@year)
    			else	
    				if @dinhdangngay='ddMMyy'
    					set @datecompare = CONCAT(@date,@month,right(@year,2))
    				else 
    					if @dinhdangngay='MMyyyy'
    						set @datecompare = CONCAT(@month,@year)
    					else	
    						if @dinhdangngay='MMyy'
    							set @datecompare = CONCAT(@month,right(@year,2))
    						else
    							if @dinhdangngay='yyyyMMdd'
    								set @datecompare = CONCAT(@year,@month,@date)
    							else 
    								if @dinhdangngay='yyMMdd'
    									set @datecompare = CONCAT(right(@year,2),@month,@date)
    								else	
    									if @dinhdangngay='yyyyMM'
    										set @datecompare = CONCAT(@year,@month)
    									else	
    										if @dinhdangngay='yyMM'
    											set @datecompare = CONCAT(right(@year,2),@month)
    										else 
    											if @dinhdangngay='yyyy'
    												set @datecompare = @year
    
    			DECLARE @sMaFull varchar(50) = concat(@machinhanh,@kituphancach1,@kihieuchungtu,@kituphancach2, @datecompare, @kituphancach3)	    			
 
				-- neu thietlapchungtu (khongco chinhanh, ngaythang, kytu ngancach)
				if @sMaFull='SQPT'
					select @Return = MAX(CAST(dbo.udf_GetNumeric(RIGHT(MaHoaDon,LEN(MaHoaDon)- len(@sMaFull)))AS float))
    				from Quy_HoaDon where SUBSTRING(MaHoaDon, 1, len(@sMaFull)) = @sMaFull and CHARINDEX('_', MaHoaDon)=0
				else
					begin
						 -- lay STTmax
					set @Return = (
							select max(maxSTT)
								from
								(								
									select *
									from
									(
										 ---- nếu chuỗi của mẫu số có chứa kí tự gạch dưới _ 
										 --> thì remove chuỗi bắt đầu từ kí tự _  (VD: HDBL_001_1 --> HDBL_001)
										 select 
											CAST(dbo.udf_GetNumeric(
													CASE WHEN CHARINDEX('_', subRight) > 0 THEN
													LEFT(subRight, CHARINDEX('_', subRight)-1) ELSE
													subRight end)
												AS float) as maxSTT 
										 from
										 (
											select RIGHT(MaHoaDon, LEN(MaHoaDon) -LEN (@sMaFull)) as subRight
											from Quy_HoaDon 
											where MaHoaDon like @sMaFull +'%' 
										) tblSubRight
									) b
								) a 
							)	
					
					end
					
    			-- lay chuoi 000
    			declare @stt int =0;
    			declare @strstt varchar (10) ='0'
    			while @stt < @dodaiSTT- 1
    				begin
    					set @strstt= CONCAT('0',@strstt)
    					SET @stt = @stt +1;
    				end 
    			declare @lenSst int = len(@strstt)
    			if	@Return is null 
    					select CONCAT(@sMaFull,left(@strstt,@lenSst-1),1) as MaxCode-- left(@strstt,@lenSst-1): bỏ bớt 1 số 0			
    			else 
    				begin
    					set @Return = @Return + 1
    					set @lenMaMax =  len(@Return)
    					select 
    						case when @lenMaMax = 1 then CONCAT(@sMaFull,left(@strstt,@lenSst-1),@Return)
    							when @lenMaMax = 2 then case when @lenSst - 2 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-2), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 3 then case when @lenSst - 3 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-3), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 4 then case when @lenSst - 4 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-4), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 5 then case when @lenSst - 5 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-5), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 6 then case when @lenSst - 6 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-6), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 7 then case when @lenSst - 7 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-7), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 8 then case when @lenSst - 8 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-8), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 9 then case when @lenSst - 9 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-9), @Return) else CONCAT(@sMaFull, @Return) end
    							when @lenMaMax = 10 then case when @lenSst - 10 > -1 then CONCAT(@sMaFull, left(@strstt,@lenSst-10), @Return) else CONCAT(@sMaFull, @Return) end
    						else '' end as MaxCode
    				end 
    		end
    	else
    		begin
    			declare @machungtu varchar(10) = (select top 1 MaLoaiChungTu from DM_LoaiChungTu where ID= @LoaiHoaDon)
    			declare @lenMaChungTu int= LEN(@machungtu)
    
    			select @Return = MAX(CAST(dbo.udf_GetNumeric(RIGHT(MaHoaDon,LEN(MaHoaDon)- @lenMaChungTu))AS float))
    			from Quy_HoaDon where SUBSTRING(MaHoaDon, 1, len(@machungtu)) = @machungtu and CHARINDEX('_', MaHoaDon)=0
    	
    			-- do dai STT (toida = 10)
    			if	@Return is null 
    					select
    						case when @lenMaChungTu = 2 then CONCAT(@machungtu, '00000000',1)
    							when @lenMaChungTu = 3 then CONCAT(@machungtu, '0000000',1)
    							when @lenMaChungTu = 4 then CONCAT(@machungtu, '000000',1)
    							when @lenMaChungTu = 5 then CONCAT(@machungtu, '00000',1)
    						else CONCAT(@machungtu,'000000',1)
    						end as MaxCode
    			else 
    				begin
    					set @Return = @Return + 1
    					set @lenMaMax = len(@Return)
    					select 
    						case when @lenMaMax = 1 then CONCAT(@machungtu,'000000000',@Return)
    							when @lenMaMax = 2 then CONCAT(@machungtu,'00000000',@Return)
    							when @lenMaMax = 3 then CONCAT(@machungtu,'0000000',@Return)
    							when @lenMaMax = 4 then CONCAT(@machungtu,'000000',@Return)
    							when @lenMaMax = 5 then CONCAT(@machungtu,'00000',@Return)
    							when @lenMaMax = 6 then CONCAT(@machungtu,'0000',@Return)
    							when @lenMaMax = 7 then CONCAT(@machungtu,'000',@Return)
    							when @lenMaMax = 8 then CONCAT(@machungtu,'00',@Return)
    							when @lenMaMax = 9 then CONCAT(@machungtu,'0',@Return)								
    						else CONCAT(@machungtu,CAST(@Return  as decimal(22,0))) end as MaxCode
    				end 
    		end
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetSoDuTheGiaTri_ofKhachHang]
    @ID_DoiTuong [uniqueidentifier],
    @DateTime [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	set @DateTime= DATEADD(DAY,1,@DateTime)

		----- get hoadon (nap, dieuchinh, hoanthe)
		select 
			hd.ID,
			hd.ID_DoiTuong,
			hd.ID_BaoHiem,
			hd.LoaiHoaDon,
			hd.PhaiThanhToan,
			hd.TongTienHang,
			hd.NgayLapHoaDon
		into #tblHD
		from dbo.BH_HoaDon hd
		where hd.ChoThanhToan = 0 
		and hd.ID_DoiTuong = @ID_DoiTuong	
		and hd.LoaiHoaDon in (22,23,32)
		and hd.NgayLapHoaDon  < @DateTime

		-----  sudungthe
		select 
			qhd.LoaiHoaDon,
			qct.ID,
			qct.ID_HoaDon,
			qct.TienThu,
			qct.HinhThucThanhToan,
			qct.ID_HoaDonLienQuan
		into #tblQCT
		from dbo.Quy_HoaDon_ChiTiet qct
		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
		where qct.ID_DoiTuong= @ID_DoiTuong
		and qct.HinhThucThanhToan = 4
		and (qhd.TrangThai is null or qhd.TrangThai='1')
		and qhd.NgayLapHoaDon < @DateTime

    	select 
    		TongThuTheGiaTri,
			TraLaiSoDu,
			SuDungThe, 
			HoanTraTheGiatri,
    		ThucThu,
			PhaiThanhToan,
			SoDuTheGiaTri,
    		iif(CongNoThe<0,0,CongNoThe) as CongNoThe
    	from
    	(
    	select 		
    		sum(TongThuTheGiaTri) - sum(TraLaiSoDu) as TongThuTheGiaTri, 
			sum(TraLaiSoDu) as TraLaiSoDu,
    		sum(SuDungThe) as SuDungThe,
    		sum(HoanTraTheGiatri) as HoanTraTheGiatri,
    		sum(ThucThu)as ThucThu,
    		sum(PhaiThanhToan)  as PhaiThanhToan,
    		SUM(TongThuTheGiaTri)- sum(TraLaiSoDu)  - SUM(SuDungThe) + SUM(HoanTraTheGiatri)  as SoDuTheGiaTri,
    		sum(PhaiThanhToan) - sum(TraLaiSoDu) - sum(ThucThu)  as CongNoThe
    	from (
    		-- so du nap the va thuc te phai thanh toan
    		SELECT 
    			sum(TongTienHang) as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			sum(hd.PhaiThanhToan) as PhaiThanhToan, -- dieu chinh the (khong lien quan den cong no)
				0 as TraLaiSoDu
    		FROM #tblHD hd
    		where hd.LoaiHoaDon in (22,23) 
    	
    
    		union all
    		-- su dung the
    		SELECT 
    			0 as TongThuTheGiaTri,
    			SUM(qct.TienThu) as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		FROM #tblQCT qct    		    		
    		WHERE  qct.LoaiHoaDon = 11 

			
    		union all
    		---- hoàn trả số dư còn trong TGT cho khách --> giảm số dư
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				SUM(hd.TongTienHang) as TraLaiSoDu
    		FROM #tblHD hd
    		where hd.LoaiHoaDon= 32
    	
    		union all
    		-- trahang: hoàn trả tiền vào TGT ---> tăng số dư
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			SUM(qct.TienThu) as HoanTraTheGiatri,
    			0 as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		FROM #tblQCT qct
    		WHERE LoaiHoaDon = 12
    	
    
    		union all
    		-- thuc thu thegiatri
    		SELECT
    			0 as TongThuTheGiaTri,
    			0 as SuDungThe,
    			0 as HoanTraTheGiatri,
    			sum(qct.TienThu) as ThucThu,
    			0 as PhaiThanhToan,
				0 as TraLaiSoDu
    		from #tblHD hd
			join Quy_HoaDon_ChiTiet qct on hd.ID= qct.ID_HoaDonLienQuan
    		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID    	
    		where qhd.NgayLapHoaDon < @DateTime 
    		and (qhd.TrangThai= '1' or qhd.TrangThai  is  null)
			and hd.LoaiHoaDon= 22    
    	
    		) tbl  
    		) tbl2
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetTongThu_fromHDDatHang]
    @IDChiNhanhs [nvarchar](max),
    @ID_Khachhang [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @tblChiNhanh table (ID_DonVi varchar(40))
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs);
    
    		---- chi get hdXuly dathang first
    		select 
    				ID,
    				TienMat, 
    				TienATM,
    				ChuyenKhoan,
    				TienDoiDiem, 
    				ThuTuThe, 									
    				TienDatCoc,
    				TienThu	
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
						
						select 
							hd.ID, hd.NgayLapHoaDon,
							qct.ID_HoaDonLienQuan,
							iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=1, qct.TienThu, 0), iif(qct.HinhThucThanhToan=1, -qct.TienThu, 0)) as TienMat,
    						iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=2, qct.TienThu, 0), iif(qct.HinhThucThanhToan=2, -qct.TienThu, 0)) as TienATM,
    						iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=3, qct.TienThu, 0), iif(qct.HinhThucThanhToan=3, -qct.TienThu, 0)) as TienCK,
    						iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=5, qct.TienThu, 0), iif(qct.HinhThucThanhToan=5, -qct.TienThu, 0)) as TienDoiDiem,
    						iif(qhd.LoaiHoaDon = 11, iif(qct.HinhThucThanhToan=4, qct.TienThu, 0), iif(qct.HinhThucThanhToan=4, -qct.TienThu, 0)) as ThuTuThe,
    						iif(qhd.LoaiHoaDon = 11, qct.TienThu, -qct.TienThu) as TienThu,
    						iif(qct.HinhThucThanhToan=6,qct.TienThu,0) as TienDatCoc
						from
						(

							select ID
							from BH_HoaDon 
							where LoaiHoaDon = 3
							and ChoThanhToan = 0
							and ID_DoiTuong like @ID_Khachhang
						) hdd
						join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDonLienQuan = hdd.ID
						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.id
						join BH_HoaDon hd on hd.ID_HoaDon= hdd.ID
						where (qhd.TrangThai= 1 Or qhd.TrangThai is null)
						and exists (select ID_DonVi from @tblChiNhanh cn2 where cn2.ID_DonVi= hd.ID_DonVi)
						and hd.NgayLapHoaDon between @FromDate and @ToDate   							
    					) d group by d.ID,d.NgayLapHoaDon,ID_HoaDonLienQuan						
    			) thuDH
    			where isFirst= 1
END");

			Sql(@"ALTER PROCEDURE [dbo].[HD_GetBuTruTraHang]
	@tblID TblID readonly,
	@LoaiHoaDonTra int = 6
AS
BEGIN

	--------- ============= Cách tính ==========
	----- I. BÙ TRỪ HÓA ĐƠN GỐC BAN ĐẦU
	----- I.1, Nếu công nợ HĐ gốc = 0, Bù trừ trả = 0
	------I.2, else:  TH1. Tổng giá trị hàng trả (all hóa đơn trả hàng từ HĐ gốc)  > Công nợ của HĐ gốc (không tính trả hàng)
	---------  ==> Gtrị bù trừ = Gtrị HĐ gốc - Nợ all HD trả + All gtrị đổi
	------  TH2. Tổng giá trị hàng trả (all hóa đơn trả hàng0 từ HĐ gốc)  < Tổng phiếu thu của HĐ gốc (đã 
	---------  ==> Gtrị bù trừ = Gtrị HĐ gốc  - gtrị all trả
	------  TH3. Tổng giá trị hàng trả (all hóa đơn trả hàng0 từ HĐ gốc)  = Tổng phiếu thu của HĐ gốc (đã 
	---------  ==> Gtrị bù trừ = Gtrị all trả
	
	----- II. BÙ TRỪ HÓA ĐƠN ĐỔI TRẢ
	------  TH1. Tổng giá trị hàng trả (all hóa đơn trả hàng từ HĐ gốc)  > Công nợ của HĐ gốc (không tính trả hàng)
	---------  ==> Nợ HĐ mới = Nợ của chính nó - Nợ hóa đơn trả (trả từ HĐ nào thì lấy công nợ của HĐ trả đó)
	------  TH2. Tổng giá trị hàng trả (all hóa đơn trả hàng từ HĐ gốc)  < Tổng phiếu thu của HĐ gốc
	---------  ==> Nợ HĐ mới = Giá trị hóa đơn mới - thuchi của chính nó 
		SET NOCOUNT ON;

		declare @LoaiThuChi int = 11
		if @LoaiHoaDonTra = 7 set @LoaiThuChi = 12

	------=========  get thongtin hdTra && hdGocBanDau (neu la HDDoiTra) ========================
	------=========  get thongtin baogia (neu HDxuly tu BaoGia) ========================
	declare @tblHD table(ID uniqueidentifier, LoaiHoaDon_TraOrBaoGia int, MaHoaDon_TraorBaoGia nvarchar(max),
				ID_HoaDonTra_orBaoGia uniqueidentifier, ID_HoaDonGocBanDau uniqueidentifier, 
				ID_DoiTuong uniqueidentifier, 
				HDMoi_PhaiThanhToan float,	
				HDMoi_NgayLapHoaDon datetime,
				HDTra_PhaiThanhToan float, HDGoc_PhaiThanhToan float)	
	 insert into @tblHD
	 select 
			hd.ID,	
			isnull(hdt.LoaiHoaDon,0) as LoaiHoaDon,
			isnull(hdt.MaHoaDon,'') as MaHoaDon_TraorBaoGia,
			hd.ID_HoaDon as ID_HoaDonTra_orBaoGia,					
			hdgoc.ID as ID_HoaDonGocBanDau,		
			hd.ID_DoiTuong,	
			hd.PhaiThanhToan as HDMoi_PhaiThanhToan,
			hd.NgayLapHoaDon,	
			iif(hdt.LoaiHoaDon= @LoaiHoaDonTra, isnull(hdt.PhaiThanhToan,0),0) as HDTra_PhaiThanhToan,		
			isnull(hdgoc.PhaiThanhToan,0) as HDGoc_PhaiThanhToan
   from dbo.BH_HoaDon hd ----- chính nó
   left join dbo.BH_HoaDon hdt on hd.ID_HoaDon= hdt.ID  and hdt.ChoThanhToan='0' ----   nếu HĐ gốc: get BaoGia, nếu HĐ đổi: get TraHang (1HD)
   left join dbo.BH_HoaDon hdgoc on hdt.ID_HoaDon = hdgoc.ID and  hdgoc.ChoThanhToan='0' --- nếu HĐ đổi: get HDGoc (1HD)
   where exists (select ID from @tblID tbl where hd.ID = tbl.ID)
 

			
	 ------ =======  if hddoi --> get thongtin hdTra && hdDoi (của HDGocBanDau)  =============
		declare @tblDoiTra table(ID uniqueidentifier, ID_HDra uniqueidentifier, ID_HDDoi uniqueidentifier,ID_DoiTuong uniqueidentifier, 
		HDDoi_PhaiThanhToan float,  HDDoi_NgayLapHoaDon datetime)
	   if (select count (*) from @tblHD where ID_HoaDonGocBanDau is not null) > 0
	   begin
			   insert into @tblDoiTra
				 --select 
					--	hdt.ID_HoaDon, --- idhdgoc 
					--	hdt.ID,				
					--	hdDoi.ID as ID_HDDoi,
					--	hdt.ID_DoiTuong,
					--	hdDoi.PhaiThanhToan as HDDoi_PhaiThanhToan,
					--	hdDoi.NgayLapHoaDon as HDDoi_NgayLapHoaDon
			  -- from dbo.BH_HoaDon hdt  
			  -- left join BH_HoaDon hdDoi on hdt.ID = hdDoi.ID_HoaDon  and hdDoi.LoaiHoaDon = 1 and hdDoi.ChoThanhToan ='0'
			  -- where exists (select ID from @tblHD tblHD
					--		   where hdt.ID_HoaDon = tblHD.ID_HoaDonGocBanDau 
					--		   and tblHD.ID_HoaDonGocBanDau is not null) ----- get all trahang of hdGoc
			  -- and hdt.LoaiHoaDon = @LoaiHoaDonTra and hdt.ChoThanhToan ='0'	  
			  select 
					hdt.ID_HoaDon, --- idhdgoc 
					hdt.ID,				
					hdDoi.ID as ID_HDDoi,
					hdt.ID_DoiTuong,
					hdDoi.PhaiThanhToan as HDDoi_PhaiThanhToan,
					hdDoi.NgayLapHoaDon as HDDoi_NgayLapHoaDon
			  from @tblHD tblHD
			  join BH_HoaDon hdt on hdt.ID_HoaDon = tblHD.ID_HoaDonGocBanDau 
			  left join BH_HoaDon hdDoi on hdt.ID = hdDoi.ID_HoaDon
				and hdDoi.LoaiHoaDon = 1 and hdDoi.ChoThanhToan ='0'
			  where hdt.LoaiHoaDon = @LoaiHoaDonTra 
			  and hdt.ChoThanhToan ='0'
			  and tblHD.ID_HoaDonGocBanDau is not null
			
	   end

	----- ======== if hdGoc: get hdTra + hdDoiMoi cua chinh no (chỉ lấy HD có trả hàng) =======
	insert into @tblDoiTra
		select 
			hdt.ID_HoaDon,  ----- idhdgoc
			hdt.ID,				
			hdDoi.ID as ID_HDDoi,
			hdt.ID_DoiTuong,
			hdDoi.PhaiThanhToan as HDDoi_PhaiThanhToan,
			hdDoi.NgayLapHoaDon as HDDoi_NgayLapHoaDon
	from @tblID tbl
	join dbo.BH_HoaDon hdt  on hdt.ID_HoaDon = tbl.ID
	left join BH_HoaDon hdDoi on hdt.ID = hdDoi.ID_HoaDon  and hdDoi.LoaiHoaDon = 1 and hdDoi.ChoThanhToan ='0'
	where 	hdt.LoaiHoaDon = @LoaiHoaDonTra and hdt.ChoThanhToan ='0'	
	and not exists (select ID from @tblHD tblHD where hdt.ID_HoaDon = tblHD.ID_HoaDonGocBanDau) 


	

		------ get phieuthu from dathang (neu hdgoc xuly tu BG) ----
		select 
			thuDH.ID,
			thuDH.TienThu as ThuDatHang,
			isFirst
		into #thuDH
		from
		(
		   select 
				hdfromBG.ID,
				hdfromBG.ID_HoaDon,
				hdfromBG.NgayLapHoaDon,
				ROW_NUMBER() OVER(PARTITION BY hdfromBG.ID_HoaDon ORDER BY hdfromBG.NgayLapHoaDon ASC) AS isFirst,	
				sum(iif(qhd.LoaiHoaDon= @LoaiThuChi, qct.TienThu, -qct.TienThu)) as TienThu
		   from
		   (
			------ get all HD xuly tu BaoGia
			   select 
					hd.ID,
					hd.ID_HoaDon,
					hd.NgayLapHoaDon
			   from dbo.BH_HoaDon hd
			   join dbo.BH_HoaDon hdd on hd.ID_HoaDon= hdd.ID
			   where exists (select ID from @tblHD tblHD where hdd.ID = tblHD.ID_HoaDonTra_orBaoGia)
			   and hd.ChoThanhToan='0'  
			   and hdd.LoaiHoaDon= 3
		   ) hdfromBG
		   join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDonLienQuan	= hdfromBG.ID_HoaDon	
			join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
			where (qhd.TrangThai is null or qhd.TrangThai='1')
			group by hdfromBG.ID,
					hdfromBG.ID_HoaDon,
					hdfromBG.NgayLapHoaDon
		) thuDH
		where thuDH.isFirst = 1

		----- get phieuthu cua hdgoc (thuchi cua chinhno)
		select 
			qct.ID_HoaDonLienQuan,
			sum(iif(qhd.LoaiHoaDon= @LoaiThuChi, qct.TienThu, -qct.TienThu)) as TienThu
		into #tblThuChinhNo
		from Quy_HoaDon_ChiTiet qct 	
		join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
		where  (qhd.TrangThai is null or qhd.TrangThai='1')
		and exists (select id from @tblHD tblHD where qct.ID_HoaDonLienQuan = tblHD.ID and qct.ID_DoiTuong = tblHD.ID_DoiTuong)
		group by qct.ID_HoaDonLienQuan
   
  	

		 ------------- tính gtri all HD đổi ----
		 select hd.ID,			
			sum(hd.HDDoi_PhaiThanhToan) as TongGiaTriDoi			
		into #allHDDoi
	   from @tblDoiTra hd	  
	   where hd.ID_HDDoi is not null ---- có trường hợp chỉ trả hàng chứ không đổi
	   	group by hd.ID		


		 ------ tính công nợ all HD trả -------
	   select hd.ID,			
			isnull(tblTra.TongTra,0) as TongGtriTra,
			isnull(tblTra.TongTra,0) - isnull(tblChi.TienChi,0) as No_HDTra
		into #tblNoTraHang
	   from (
		select distinct ID from @tblDoiTra ----- distinct vì nếu trả hàng 2 lần sẽ bị douple cùng 1 ID_HDGoc
	   ) hd
	   left join
	   (
		---- tong giatritra			
		select 
				tNew.ID,
				sum(hdt.PhaiThanhToan) as TongTra
			from @tblDoiTra tNew	
			join dbo.BH_HoaDon hdt on hdt.ID_HoaDon = tNew.ID and tNew.ID_HDra = hdt.ID
			where hdt.ChoThanhToan='0'
			and exists (select tbl.ID_HDra from @tblDoiTra tbl where hdt.ID_HoaDon = tbl.ID)
			group by tNew.ID
	   ) tblTra on hd.ID = tblTra.ID
	   left join 
	   (
		  ---- tongchi all HDTra ----		
			select tNew.ID,
					sum(iif(qhd.LoaiHoaDon = @LoaiThuChi, -qct.TienThu,  qct.TienThu)) as TienChi						
			from @tblDoiTra tNew 
			join Quy_HoaDon_ChiTiet qct	on tNew.ID_HDra	= qct.ID_HoaDonLienQuan	
			join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
			where (qhd.TrangThai is null or qhd.TrangThai='1')
			and exists (select tbl.ID_HDra from @tblDoiTra tbl where qct.ID_HoaDonLienQuan = tbl.ID_HDra and qct.ID_DoiTuong = tbl.ID_DoiTuong)
			group by tNew.ID
	   ) tblChi on hd.ID = tblChi.ID


	   ------ get luyke trahang (of HDDoi) ----
		select 
			ID,
			LuyKeTraHang,
			LuyKeTraHang - TongChi as CongNoLuyKeTra
		into #tblLuyKeTra
		from
		(
			select hdt.ID_HoaDon,	
				hdt.MaHoadon,
				hdt.ID,
				hdt.NgayLapHoaDon,
				hdt.PhaiThanhToan,
				sum (isnull(chiLuyKe.TongChi,0)) OVER(PARTITION BY hdt.ID_HoaDon ORDER BY hdt.NgayLapHoaDon) as TongChi,
				sum (hdt.PhaiThanhToan) OVER(PARTITION BY hdt.ID_HoaDon ORDER BY hdt.NgayLapHoaDon)	as LuyKeTraHang
	
			from BH_HoaDon hdt	
			left join (
				---- tongchi theo luyke hdtra
				select qct.ID_HoaDonLienQuan, sum(qct.TienThu) as TongChi
				from Quy_HoaDon_ChiTiet qct	
				join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
				where (qhd.TrangThai is null or qhd.TrangThai='1')		
				group by qct.ID_HoaDonLienQuan
				) chiLuyKe on hdt.ID = chiLuyKe.ID_HoaDonLienQuan
			where hdt.ChoThanhToan='0'
			and exists
			(select ID from @tblHD tbl where hdt.ID_HoaDon= tbl.ID_HoaDonGocBanDau and tbl.ID_HoaDonGocBanDau is not null
			and hdt.NgayLapHoaDon < tbl.HDMoi_NgayLapHoaDon	)					
		)lkTra

		 ------ get luyke hdDoi  ----
		 select ID_HDDoi,
			 sum(LuyKeDoi) as  LuyKeDoi,
			sum(TongThu) as TongThu,
			sum(LuyKeDoi) - sum(TongThu)  as CongNoLuyKeDoi
		 into #tblLuyKeDoi
		 from
		 (
		select 
			ID_HDDoi,
			
			LuyKeDoi,
			TongThu
		
		from
		(
		select hdDoi.ID_HDDoi,			
			hdDoi.ID,
			hdDoi.HDDoi_NgayLapHoaDon,
			hdDoi.HDDoi_PhaiThanhToan,		
			sum (isnull(chiLuyKe.TongThu,0)) OVER(PARTITION BY hdDoi.ID ORDER BY hdDoi.HDDoi_NgayLapHoaDon) as TongThu,
			sum ( hdDoi.HDDoi_PhaiThanhToan)  OVER(PARTITION BY hdDoi.ID ORDER BY hdDoi.HDDoi_NgayLapHoaDon)	as LuyKeDoi	
		from @tblDoiTra hdDoi		
		left join (
			---- tongchi theo luyke hdtra
			select qct.ID_HoaDonLienQuan, sum(qct.TienThu) as TongThu
			from Quy_HoaDon_ChiTiet qct	
			join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
			where (qhd.TrangThai is null or qhd.TrangThai='1')		
			group by qct.ID_HoaDonLienQuan
		) chiLuyKe on hdDoi.ID_HDDoi = chiLuyKe.ID_HoaDonLienQuan
		where exists (select ID from #tblLuyKeTra tbl where hdDoi.ID_HDra= tbl.ID )
			and ID_HDDoi is not null	
		 
		
		)lkDoi 
		
		union all

		select tblHD.ID_HDDoi as ID_HDDoi,
				-tblHD.HDDoi_PhaiThanhToan as LuyKeDoi ,
				-isnull(thu.TienThu,0)  as TongThu
		from @tblDoiTra tblHD
		left join #tblThuChinhNo thu on tblHD.ID_HDDoi= thu.ID_HoaDonLienQuan
		where ID_HDDoi is not null
		) a
		group by a.ID_HDDoi
		


			select distinct
				tblLast.ID,
				tblLast.MaHoaDon_TraorBaoGia,
				tblLast.LoaiHoaDon_TraOrBaoGia,			
				iif(tblLast.LoaiHoaDon_TraOrBaoGia != @LoaiHoaDonTra,
					------ butrutra - HDGoc -----
					iif(tblLast.CongNoChinhNo = 0, 0,
					case
						when tblLast.TongGtriTra > tblLast.CongNoChinhNo then iif(tblLast.CongNoChinhNo > 0,tblLast.CongNoChinhNo,  tblLast.No_HDTra - tblLast.TongGiaTriDoi)
						when tblLast.TongGtriTra < tblLast.CongNoChinhNo then tblLast.TongGtriTra
					else tblLast.TongGtriTra
					end),
					case 
						when tblLast.CongNoHDGocCu = 0 then 
							iif(tblLast.LuyKeTraHang = 0,
								iif( tblLast.HDTra_PhaiThanhToan > tblLast.HDMoi_PhaiThanhToan,tblLast.HDMoi_PhaiThanhToan, tblLast.HDTra_PhaiThanhToan) , ---- tranhanh
							tblLast.CongNoLuyKeTra - tblLast.CongNoLuyKeDoi)
						when tblLast.CongNoHDGocCu != 0 then iif( tblLast.CongNoHDGocCu > tblLast.CongNoLuyKeTra, 0, 
							 tblLast.CongNoLuyKeTra - tblLast.CongNoHDGocCu - tblLast.CongNoLuyKeDoi)
						end
					) 
				as BuTruTraHang,
				tblLast.KhachDaTra
		from
		(
		select 
			
			tblHD.ID,		
			tblHD.ID_HoaDonTra_orBaoGia,
			--hd.MaHoaDon,
			--hd.NgayLapHoaDon,
			tblHD.MaHoaDon_TraorBaoGia,
			tblHD.LoaiHoaDon_TraOrBaoGia,
			tblHD.HDTra_PhaiThanhToan,
			isnull(thuchiChinhNo.TienThu,0) as KhachDaTra,	
			tblHD.HDMoi_PhaiThanhToan,
			tblHD.HDMoi_PhaiThanhToan - isnull(thuchiChinhNo.TienThu,0) as CongNoChinhNo,
				
			isnull(th.TongGtriTra,0) as TongGtriTra,
			isnull(th.No_HDTra,0) as No_HDTra,

			isnull(luykeTra.LuyKeTraHang,0) as LuyKeTraHang,	
			isnull(luykeTra.CongNoLuyKeTra,0) as CongNoLuyKeTra,

			isnull(luykeDoi.CongNoLuyKeDoi,0) as CongNoLuyKeDoi,

			isnull(doi.TongGiaTriDoi,0) as TongGiaTriDoi,
			isnull(tblHD.HDGoc_PhaiThanhToan,0) - isnull(thuhdGoc.ThuHDGoc,0) as CongNoHDGocCu

		from @tblHD tblHD
		--join BH_HoaDon hd on tblHD.ID= hd.ID
		left join
		 (
		  select 
				tc.ID_HoaDonLienQuan,
				sum(tc.TienThu) as TienThu
		  from
		  (
				select tc1.ID_HoaDonLienQuan,
						tc1.TienThu
				from #tblThuChinhNo tc1

				union all

				select tc2.ID,
						tc2.ThuDatHang
				from #thuDH tc2
			) tc group by tc.ID_HoaDonLienQuan

	  ) thuchiChinhNo on tblHD.ID =thuchiChinhNo.ID_HoaDonLienQuan
	  left join
	  (	
		  select 
			thuhdGoc.ID_HoaDonLienQuan,
			sum(TienThu) as ThuHDGoc
		  from
		  (
			----- thu hdGoc ---
			select qct.ID_HoaDonLienQuan,
				sum(iif(qhd.LoaiHoaDon = @LoaiThuChi, qct.TienThu, - qct.TienThu)) as TienThu		
			from Quy_HoaDon_ChiTiet qct					
			join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID	
			where (qhd.TrangThai is null or qhd.TrangThai='1')
			and exists 
				(select tblHD.ID_HoaDonGocBanDau from @tblHD tblHD 
				where qct.ID_HoaDonLienQuan = tblHD.ID_HoaDonGocBanDau and qct.ID_DoiTuong = tblHD.ID_DoiTuong
				)
			group by qct.ID_HoaDonLienQuan 
			union all

			select tc2.ID,
					tc2.ThuDatHang
			from #thuDH tc2

			) thuhdGoc group by thuhdGoc.ID_HoaDonLienQuan 
	  ) thuhdGoc on tblHD.ID_HoaDonGocBanDau = thuhdGoc.ID_HoaDonLienQuan
	   left join #tblNoTraHang th on tblHD.ID = th.ID or tblHD.ID_HoaDonGocBanDau = th.ID
	   left join #tblLuyKeTra luykeTra on tblHD.ID_HoaDonTra_orBaoGia = luykeTra.ID
	   left join #allHDDoi doi on tblHD.ID = doi.ID
	     left join #tblLuyKeDoi luykeDoi on tblHD.ID = luykeDoi.ID_HDDoi
	) tblLast 
	


	  drop table #thuDH
	  drop table #tblNoTraHang
	  drop table #allHDDoi
	  drop table #tblLuyKeTra
	  drop table #tblThuChinhNo

 
END");

			Sql(@"ALTER PROCEDURE [dbo].[Load_DMHangHoa_TonKho]
	 @ID_ChiNhanh [uniqueidentifier]
AS
BEGIN

	SET NOCOUNT ON;
	declare @dateNow datetime = FORMAT(getdate(),'yyyyMMdd')
	declare @next10Year Datetime = FORMAT(dateadd(year,10, getdate()),'yyyyMMdd')

		select 
			dhh1.ID,
			dvqd1.ID as ID_DonViQuiDoi,		
			MAX(ROUND(ISNULL(tk.TonKho,0),2)) as TonKho,
			MAX(CAST(ROUND(( ISNULL(gv.GiaVon,0)), 0) as float)) as GiaVon,
			TenHangHoa,
			MaHangHoa,
			LaDonViChuan,  
			LaHangHoa,
			dhh1.TonToiThieu,
			ID_NhomHang as ID_NhomHangHoa,
			ISNULL(QuanLyTheoLoHang,'0') as QuanLyTheoLoHang,
			Case when dhh1.LaHangHoa='1' then 0 else CAST(ISNULL(dhh1.ChiPhiThucHien,0) as float) end as PhiDichVu,
			Case when dhh1.LaHangHoa='1' then '0' else ISNULL(dhh1.ChiPhiTinhTheoPT,'0') end as LaPTPhiDichVu,
			ISNULL(lh1.ID,  NEWID()) as ID_LoHang,
			case when MAX(ISNULL(QuyCach,0)) = 0 then MAX(TyLeChuyenDoi) else MAX(QuyCach) * MAX(TyLeChuyenDoi) end as QuyCach,	
			MAX(ISNULL(TyLeChuyenDoi,0)) as TyLeChuyenDoi, 		
			isnull(TenHangHoa_KhongDau,'') as TenHangHoa_KhongDau,
			CONCAT(MaHangHoa, ' ' , lower(MaHangHoa) ,' ', TenHangHoa, ' ', TenHangHoa_KhongDau,' ',
			MAX(MaLoHang),' ', Cast(max(GiaBan) as decimal(22,0)), MAX(ISNULL(dvqd1.ThuocTinhGiaTri,''))) as Name,
    		MAX(ISNULL(dvqd1.ThuocTinhGiaTri,'')) as ThuocTinh_GiaTri,
    		MAX(GiaBan) as GiaBan, 
    		MAX (TenDonViTinh) as TenDonViTinh, 	
			case when MAX(ISNULL(an.URLAnh,'')) = '' then '' else 'CssImg' end as CssImg,		
    		MAX(ISNULL(an.URLAnh,'')) as SrcImage, 		
    		MAX(ISNULL(MaLoHang,'')) as MaLoHang,
    		MAX(NgaySanXuat) as NgaySanXuat,
    		MAX(NgayHetHan) as NgayHetHan,
			MAX(ISNULL(DonViTinhQuyCach,'')) as DonViTinhQuyCach,
			MAX(ISNULL(ThoiGianBaoHanh,0)) as ThoiGianBaoHanh,
			MAX(ISNULL(LoaiBaoHanh,0)) as LoaiBaoHanh,
			MAX(ISNULL(SoPhutThucHien,0)) as SoPhutThucHien, 
			MAX(ISNULL(dhh1.GhiChu,'')) as GhiChuHH ,
			MAX(ISNULL(dhh1.DichVuTheoGio,0)) as DichVuTheoGio, 
			MAX(ISNULL(dhh1.DuocTichDiem,0)) as DuocTichDiem, 
			MAX(ISNULL(dhh1.DichVuTheoGio,0)) as DichVuTheoGio, 
			MAX(ISNULL(dhh1.ChietKhauMD_NV,0)) as ChietKhauMD_NV, 
			ISNULL(dhh1.ChietKhauMD_NVTheoPT,'1') as ChietKhauMD_NVTheoPT, 
			0 as SoGoiDV,
			@next10Year as HanSuDungGoiDV_Min,
			'' as BackgroundColor,
			iif(dhh1.LoaiHangHoa is null, iif(dhh1.LaHangHoa='1',1,2), dhh1.LoaiHangHoa) as LoaiHangHoa,
			isnull(nhom.TenNhomHangHoa,N'Nhóm mặc định') as TenNhomHangHoa
		from DonViQuiDoi dvqd1 
		join DM_HangHoa dhh1 on dvqd1.ID_HangHoa = dhh1.ID
		left join DM_NhomHangHoa nhom on dhh1.ID_NhomHang = nhom.ID
		left join DM_LoHang lh1 on dvqd1.ID_HangHoa = lh1.ID_HangHoa and lh1.TrangThai='1'
		left join DM_HangHoa_TonKho tk on (dvqd1.ID = tk.ID_DonViQuyDoi and (lh1.ID = tk.ID_LoHang or lh1.ID is null) and  tk.ID_DonVi = @ID_ChiNhanh)
		left join DM_HangHoa_Anh an on (dvqd1.ID_HangHoa = an.ID_HangHoa and (an.sothutu = 1 or an.ID is null))		
		left join DM_GiaVon gv on (dvqd1.ID = gv.ID_DonViQuiDoi and (lh1.ID = gv.ID_LoHang or lh1.ID is null) and gv.ID_DonVi = @ID_ChiNhanh)
		where (dvqd1.xoa ='0'  or dvqd1.Xoa is null)
		and dhh1.TheoDoi = '1'	
		and dhh1.DuocBanTrucTiep = '1' --- chi lay hanghoa DuocBanTrucTiep
		and (dhh1.LaHangHoa = 0 or (dhh1.LaHangHoa = 1 and tk.TonKho is not null)) -- chi lay HangHoa neu exsit in DM_TonKho_HangHoa
		and (lh1.NgayHetHan is null or (lh1.NgayHetHan >= @dateNow))		
		group by dhh1.ID, dvqd1.ID, lh1.ID, MaHangHoa,ID_NhomHang,TenHangHoa,TenHangHoa_KhongDau,TenHangHoa_KyTuDau,
		LaDonViChuan,LaHangHoa,ChiPhiThucHien,ChiPhiTinhTheoPT, dhh1.QuanLyTheoLoHang, dhh1.TonToiThieu, dhh1.LoaiHangHoa,nhom.TenNhomHangHoa,  dhh1.ChietKhauMD_NVTheoPT
		order by MaHangHoa,NgayHetHan
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhMonth_DoanhThuBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
	SET NOCOUNT ON;
    SELECT
    	a.ThangLapHoaDon,
    	CAST(ROUND(SUM(a.DoanhThu), 0) as float) as DoanhThu,
		CAST(ROUND(SUM(a.GiaVonGDV), 0) as float) as GiaVonGDV,
    	CAST(ROUND(SUM(a.GiaTriTra), 0) as float) as GiaTriTra,
    	CAST(ROUND(SUM(a.GiamGiaHDB - a.GiamGiaHDT), 0) as float) as GiamGiaHD
    	FROM
    	(
    		Select 
    		DATEPART(MONTH, hd.NgayLapHoaDon) as ThangLapHoaDon,
    		hd.LoaiHoaDon,
    		Case When hd.LoaiHoaDon in (1,19,25) and hdct.ChatLieu != 4 then hdct.ThanhTien else 0 end as DoanhThu,
			Case When (hd.LoaiHoaDon = 1) and hdct.ID_ChiTietGoiDV is not null and (hdct.ChatLieu is null or hdct.ChatLieu = 4) then ISNULL(hdct.SoLuong * hdct.GiaVon ,0) else 0 end as GiaVonGDV,
    		Case When hd.LoaiHoaDon = 6 then hdct.ThanhTien else 0 end as GiaTriTra,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon in (1,19,25) then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDB,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon = 6 then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDT
    		From BH_HoaDon hd
			inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
    		where hd.LoaiHoaDon in (1,19,25,6)
    		and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    		and hd.ChoThanhToan = 0
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh)) 
			AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID)
			AND (hdct.ID_ParentCombo IS NULL OR hdct.ID_ParentCombo = hdct.ID)
			UNION ALL
			select MONTH(hdxk.NgayLapHoaDon) AS ThangLapHoaDon, 
			25, 
			0 AS DoanhThu, 
			hdxkct.SoLuong * hdxkct.GiaVon AS GiaVonGDV,
			0 AS GiaTriTra,
			0 AS GiamGiaHDB,
			0 AS GiamGiaHDT
			from Gara_PhieuTiepNhan ptn
			inner join BH_HoaDon hdxk ON ptn.ID = hdxk.ID_PhieuTiepNhan
			INNER JOIN BH_HoaDon_ChiTiet hdxkct ON hdxk.ID = hdxkct.ID_HoaDon
			where 
			--hdsc.LoaiHoaDon = 25 and hdsc.ChoThanhToan = 0 and
			hdxk.LoaiHoaDon = 8 AND
			hdxk.ChoThanhToan = 0 AND YEAR(hdxk.NgayLapHoaDon) = @year
			and hdxk.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    	) as a
    	GROUP BY
    	a.ThangLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhMonth_GiaVonBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @tblThang TABLE (ThangLapHoaDon INT, TongGiaVonBan FLOAT, TongGiaVonTra FLOAT);
INSERT INTO @tblThang (ThangLapHoaDon, TongGiaVonBan, TongGiaVonTra)
VALUES (1, 0, 0), (2,0,0), (3,0,0), (4,0,0), (5,0,0), (6,0,0), (7,0,0), (8,0,0), (9,0,0), (10,0,0), (11,0,0), (12,0,0);
	SELECT 
	b.ThangLapHoaDon,
	SUM(CAST(ROUND(b.TongGiaVonBan, 0) as float)) as TongGiaVonBan,
	SUM(CAST(ROUND(b.TongGiaVonTra , 0) as float)) as TongGiaVonTra
	FROM
	(
    SELECT
    	a.ThangLapHoaDon,
		SUM((a.SoLuongBan - a.SoLuongTra) * a.GiaVonBan) as TongGiaVonBan,
		SUM(a.SoLuongTraNhanh * a.GiaVonTraNhanh) as TongGiaVonTra
    	FROM
    	(
    		Select 
    		DATEPART(MONTH, hd.NgayLapHoaDon) as ThangLapHoaDon,
			hdct.ID_DonViQuiDoi,
			hdct.SoLuong as SoLuongBan,
			ISNULL(hdct.GiaVon, 0) as GiaVonBan,
			0 as SoLuongTra,
			0 as GiaVonTra,
    		0 as SoLuongTraNhanh,
			0 as GiaVonTraNhanh
    		From BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		where ((hd.LoaiHoaDon in (1,19)
			and hdct.ID_ChiTietGoiDV is null) OR (hd.LoaiHoaDon = 1 AND hdct.ID_ChiTietGoiDV is not null and  hdct.ChatLieu = 3))
    		and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    		and hd.ChoThanhToan = 0
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			Union all
			Select 
    		DATEPART(MONTH, hdb.NgayLapHoaDon) as ThangLapHoaDon,
			hdct.ID_DonViQuiDoi,
			0 as SoLuongBan,
			ISNULL(ctb.GiaVon, 0) as GiaVonBan,
			hdct.SoLuong as SoLuongTra,
			0 as GiaVonTra,
    		0 as SoLuongTraNhanh,
			0 as GiaVonTraNhanh
    		From BH_HoaDon hdb
			inner join BH_HoaDon hdt on hdb.ID = hdt.ID_HoaDon
    		inner join BH_HoaDon_ChiTiet hdct on hdt.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
			join BH_HoaDon_ChiTiet ctb on (ctb.ID_HoaDon =  hdt.ID_HoaDon and ctb.ID_DonViQuiDoi = hdct.ID_DonViQuiDoi and ((ctb.ID_LoHang = hdct.ID_LoHang) or (ctb.ID_LoHang is null and hdct.ID_LoHang is null))) 
    		where ((hdb.LoaiHoaDon in (1,19)
			and hdct.ID_ChiTietGoiDV is null) OR (hdb.LoaiHoaDon = 1 AND hdct.ID_ChiTietGoiDV is not null and  hdct.ChatLieu = 3))
    		and DATEPART(YEAR, hdt.NgayLapHoaDon) = @year
    		and hdt.ChoThanhToan = 0
    		and hdb.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			UNION ALL
    		SELECT
    		DATEPART(MONTH, hdb.NgayLapHoaDon) as ThangLapHoaDon,
			hdct.ID_DonViQuiDoi,
			0 as SoLuongBan,
			0 as GiaVonBan,
			0 as SoLuongTra,
			0 as GiaVonTra,
			hdct.SoLuong as SoLuongTraNhanh,
			ISNULL(hdct.GiaVon, 0) as GiaVonTraNhanh
    		FROM
    		BH_HoaDon hdb
    		join BH_HoaDon_ChiTiet hdct on hdb.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		where DATEPART(YEAR, hdb.NgayLapHoaDon) = @year
    		and hdb.ChoThanhToan = 0
    		and hdb.ID_DonVi in (Select * from splitstring(@ID_ChiNhanh))
    		and hdb.LoaiHoaDon = 6
			and hdb.ID_HoaDon is null
    	) as a
    	GROUP BY a.ID_DonViQuiDoi, a.ThangLapHoaDon
		UNION ALL SELECT * FROM @tblThang)
		as b
		
		GROUP BY b.ThangLapHoaDon
END

");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhMonth_SoQuyBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;
    SELECT
    	a.ThangLapHoaDon,
    	CAST(ROUND(SUM(a.ThuNhapKhac), 0) as float) as ThuNhapKhac,
    	CAST(ROUND(SUM(a.ChiPhiKhac), 0) as float) as ChiPhiKhac,
    	CAST(ROUND(SUM(a.PhiTraHangNhap), 0) as float) as PhiTraHangNhap,
    	CAST(ROUND(SUM(a.KhachThanhToan), 0) as float) as KhachThanhToan
    	FROM
    	(
    		Select 
    		MONTH(qhd.NgayLapHoaDon) as ThangLapHoaDon,
    		Case when (hd.LoaiHoaDon is null and qhd.LoaiHoaDon = 11) then qhdct.TienThu else 0 end as ThuNhapKhac,
    		Case when (hd.LoaiHoaDon is null and qhd.LoaiHoaDon = 12)
			or (hd.LoaiHoaDon in (1,3,19) and qhd.LoaiHoaDon = 12) then qhdct.TienThu else 0 end as ChiPhiKhac,
    		Case when (hd.LoaiHoaDon = 7) then qhdct.TienThu else 0 end as PhiTraHangNhap,
    		Case when (hd.LoaiHoaDon in (1,3,25)) and qhd.LoaiHoaDon = 11 then qhdct.TienThu else 0 end as KhachThanhToan
    		From Quy_HoaDon qhd
    		inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    		left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    		where (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    		and YEAR(qhd.NgayLapHoaDon) = @year
    		and (qhd.HachToanKinhDoanh = 1)
    		and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			and (qhdct.DiemThanhToan is null OR qhdct.DiemThanhToan = 0) and qhdct.LoaiThanhToan != 1
    	) as a
    	GROUP BY
    	a.ThangLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhYear_DoanhThuBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;
    SELECT
    	a.NamLapHoaDon,
    	CAST(ROUND(SUM(a.DoanhThu), 0) as float) as DoanhThu,
		CAST(ROUND(SUM(a.GiaVonGDV), 0) as float) as GiaVonGDV,
    	CAST(ROUND(SUM(a.GiaTriTra), 0) as float) as GiaTriTra,
    	CAST(ROUND(SUM(a.GiamGiaHDB - a.GiamGiaHDT), 0) as float) as GiamGiaHD
    	FROM
    	(
    		Select 
    		DATEPART(YEAR, hd.NgayLapHoaDon) as NamLapHoaDon,
    		hd.LoaiHoaDon,
    		Case When hd.LoaiHoaDon in (1,19,25) and hdct.ChatLieu != 4 then hdct.ThanhTien else 0 end as DoanhThu,
			Case When (hd.LoaiHoaDon = 1) and hdct.ID_ChiTietGoiDV is not null and (hdct.ChatLieu is null or hdct.ChatLieu = 4) then ISNULL(hdct.SoLuong * hdct.GiaVon ,0) else 0 end as GiaVonGDV,
    		Case When hd.LoaiHoaDon = 6 then hdct.ThanhTien else 0 end as GiaTriTra,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon in (1,19,25) then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDB,
			Case when hd.TongTienHang != 0 and hd.LoaiHoaDon = 6 then hdct.ThanhTien * ((ISNULL(hd.TongGiamGia, 0) + ISNULL(hd.KhuyeMai_GiamGia, 0)) / ISNULL(hd.TongTienHang, 0)) else 0 end as GiamGiaHDT
    		From BH_HoaDon hd
			inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
    		where hd.LoaiHoaDon in (1,6,19,25)
    		and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    		and hd.ChoThanhToan = 0
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			AND (hdct.ID_ChiTietDinhLuong IS NULL OR hdct.ID_ChiTietDinhLuong = hdct.ID)
			AND (hdct.ID_ParentCombo IS NULL OR hdct.ID_ParentCombo = hdct.ID)
			UNION ALL
			select YEAR(hdxk.NgayLapHoaDon) AS ThangLapHoaDon, 
			25, 
			0 AS DoanhThu, 
			hdxkct.SoLuong * hdxkct.GiaVon AS GiaVonGDV,
			0 AS GiaTriTra,
			0 AS GiamGiaHDB,
			0 AS GiamGiaHDT
			from Gara_PhieuTiepNhan ptn
			inner join BH_HoaDon hdxk ON ptn.ID = hdxk.ID_PhieuTiepNhan
			INNER JOIN BH_HoaDon_ChiTiet hdxkct ON hdxk.ID = hdxkct.ID_HoaDon
			where hdxk.LoaiHoaDon = 8
			and hdxk.ChoThanhToan = 0 AND YEAR(hdxk.NgayLapHoaDon) = @year
			and hdxk.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    	) as a
    	GROUP BY
    	a.NamLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhYear_GiaVonBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @tblThang TABLE (NamLapHoaDon INT, TongGiaVonBan FLOAT, TongGiaVonTra FLOAT);
INSERT INTO @tblThang (NamLapHoaDon, TongGiaVonBan, TongGiaVonTra)
VALUES (@year, 0, 0);
	SELECT 
	b.NamLapHoaDon,
	SUM(CAST(ROUND(b.TongGiaVonBan, 0) as float)) as TongGiaVonBan,
	SUM(CAST(ROUND(b.TongGiaVonTra , 0) as float)) as TongGiaVonTra
	FROM
	(
    SELECT
    	a.NamLapHoaDon,
		SUM((a.SoLuongBan - (a.SoLuongTra)) * a.GiaVonBan) as TongGiaVonBan,
		SUM(a.SoLuongTraNhanh * a.GiaVonTraNhanh) as TongGiaVonTra
    	FROM
    	(
    		Select 
    		DATEPART(YEAR, hd.NgayLapHoaDon) as NamLapHoaDon,
			hdct.ID_DonViQuiDoi,
			hdct.SoLuong as SoLuongBan,
			ISNULL(hdct.GiaVon, 0) as GiaVonBan,
			0 as SoLuongTra,
			0 as GiaVonTra,
    		0 as SoLuongTraNhanh,
			0 as GiaVonTraNhanh
    		From BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		where ((hd.LoaiHoaDon in (1,19)
			and hdct.ID_ChiTietGoiDV is null) OR (hd.LoaiHoaDon = 1 AND hdct.ID_ChiTietGoiDV is not null and  hdct.ChatLieu = 3))
    		and DATEPART(YEAR, hd.NgayLapHoaDon) = @year
    		and hd.ChoThanhToan = 0
    		and hd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			Union all
			Select 
    		DATEPART(YEAR, hdt.NgayLapHoaDon) as NamLapHoaDon,
			hdct.ID_DonViQuiDoi,
			NULL as SoLuongBan,
			ISNULL(ctb.GiaVon, 0) as GiaVonBan,
			hdct.SoLuong as SoLuongTra,
			0 as GiaVonTra,
    		0 as SoLuongTraNhanh,
			0 as GiaVonTraNhanh
    		From BH_HoaDon hdb
			inner join BH_HoaDon hdt on hdb.ID = hdt.ID_HoaDon
    		inner join BH_HoaDon_ChiTiet hdct on hdt.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
			join BH_HoaDon_ChiTiet ctb on (ctb.ID_HoaDon =  hdt.ID_HoaDon and ctb.ID_DonViQuiDoi = hdct.ID_DonViQuiDoi and ((ctb.ID_LoHang = hdct.ID_LoHang) or (ctb.ID_LoHang is null and hdct.ID_LoHang is null))) 
    		where ((hdb.LoaiHoaDon in (1,19)
			and hdct.ID_ChiTietGoiDV is null) OR (hdb.LoaiHoaDon = 1 AND hdct.ID_ChiTietGoiDV is not null and  hdct.ChatLieu = 3))
    		and DATEPART(YEAR, hdt.NgayLapHoaDon) = @year
    		and hdt.ChoThanhToan = 0
    		and hdb.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			UNION ALL
    		SELECT
    		DATEPART(YEAR, hdb.NgayLapHoaDon) as NamLapHoaDon,
			hdct.ID_DonViQuiDoi,
			0 as SoLuongBan,
			0 as GiaVonBan,
			0 as SoLuongTra,
			0 as GiaVonTra,
			hdct.SoLuong as SoLuongTraNhanh,
			ISNULL(hdct.GiaVon, 0) as GiaVonTraNhanh
    		FROM
    		BH_HoaDon hdb
    		join BH_HoaDon_ChiTiet hdct on hdb.ID = hdct.ID_HoaDon and (hdct.ID_ChiTietDinhLuong = hdct.ID or hdct.ID_ChiTietDinhLuong is null)
			inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		where DATEPART(YEAR, hdb.NgayLapHoaDon) = @year
    		and hdb.ChoThanhToan = 0
    		and hdb.ID_DonVi in (Select * from splitstring(@ID_ChiNhanh))
    		and hdb.LoaiHoaDon = 6
			and hdb.ID_HoaDon is null
    	) as a
    	GROUP BY a.ID_DonViQuiDoi, a.NamLapHoaDon
		UNION ALL SELECT * FROM @tblThang
		) as b
		GROUP BY b.NamLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportTaiChinhYear_SoQuyBanHang]
    @year [int],
    @ID_ChiNhanh [nvarchar](max)
AS
--EXEC ReportTaiChinhYear_SoQuyBanHang '2021', 'a31fa9bc-dd97-47f8-9901-f19efc4fa831'
BEGIN
SET NOCOUNT ON;
    SELECT
    	a.NamLapHoaDon,
    	CAST(ROUND(SUM(a.ThuNhapKhac), 0) as float) as ThuNhapKhac,
    	CAST(ROUND(SUM(a.ChiPhiKhac), 0) as float) as ChiPhiKhac,
    	CAST(ROUND(SUM(a.PhiTraHangNhap), 0) as float) as PhiTraHangNhap,
    	CAST(ROUND(SUM(a.KhachThanhToan), 0) as float) as KhachThanhToan
    	FROM
    	(
    		Select 
    		DATEPART(YEAR, qhd.NgayLapHoaDon) as NamLapHoaDon,
    		Case when (hd.LoaiHoaDon is null and qhd.LoaiHoaDon = 11) then qhdct.TienThu else 0 end as ThuNhapKhac,
    		Case when (hd.LoaiHoaDon is null and qhd.LoaiHoaDon = 12) or (hd.LoaiHoaDon in (3,19) and qhd.LoaiHoaDon = 12) then qhdct.TienThu else 0 end as ChiPhiKhac,
    		Case when (hd.LoaiHoaDon = 7) then qhdct.TienThu else 0 end as PhiTraHangNhap,
    		Case when hd.LoaiHoaDon in (1,3,25) and qhd.LoaiHoaDon = 11 then qhdct.TienThu else 0 end as KhachThanhToan
    		From Quy_HoaDon qhd
    		inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    		left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    		where (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    		and DATEPART(YEAR, qhd.NgayLapHoaDon) = @year
    		and (qhd.HachToanKinhDoanh = 1)
    		and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
			and (qhdct.DiemThanhToan is null OR qhdct.DiemThanhToan = 0) and qhdct.LoaiThanhToan != 1
    	) as a
    	GROUP BY
    	a.NamLapHoaDon
END");

			Sql(@"ALTER PROCEDURE [dbo].[Search_DMHangHoa_TonKho]
    @MaHH [nvarchar](max),
    @MaHH_TV [nvarchar](max),
    @ID_ChiNhanh [uniqueidentifier],
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
SET NOCOUNT ON;
    DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select top 1
    						Case when nd.LaAdmin = '1' then '1' else
    						Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    						From
    						HT_NguoiDung nd	
    						where nd.ID = @ID_NguoiDung)
		DECLARE @tablename TABLE(Name [nvarchar](max))	 
    	DECLARE @tablenameChar TABLE(  Name [nvarchar](max))
  
    	DECLARE @count int
    	DECLARE @countChar int
    	INSERT INTO @tablename(Name) select  Name from [dbo].[splitstring](@MaHH+',') where Name!='';
    	INSERT INTO @tablenameChar(Name) select  Name from [dbo].[splitstring](@MaHH_TV+',') where Name!='';
    	Select @count =  (Select count(*) from @tablename);
    	Select @countChar =   (Select count(*) from @tablenameChar);

select qd.ID as ID_DonViQuiDoi,
		MaHangHoa, TenHangHoa, TenHangHoa_KhongDau, TenHangHoa_KyTuDau,TenDonViTinh, ThuocTinhGiaTri as ThuocTinh_GiaTri,
		CONCAT(TenHangHoa,' ', ThuocTinhGiaTri,' ', case when TenDonViTinh='' or TenDonViTinh is null then '' else ' (' + TenDonViTinh + ')' end) as TenHangHoaFull,
		ISNULL(tk.TonKho,0) as TonCuoiKy,
		CAST(ROUND((qd.GiaBan), 0) as float) as GiaBan,		
		case when @XemGiaVon= '1' then CAST(ROUND((ISNULL(gv.GiaVon,0)), 0) as float) else 0 end as GiaVon,
		case when @XemGiaVon= '1' then	
			case when hh.LaHangHoa='1' then CAST(ROUND((ISNULL(gv.GiaVon,0)), 0) as float)
			else CAST(ROUND((ISNULL(tblDVu.GiaVon,0)), 0) as float) end
		else 0 end as GiaVon,
		gv.ID_DonVi, hh.LaHangHoa
	from DonViQuiDoi qd 
	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
	left join DM_HangHoa_TonKho tk on qd.ID= tk.ID_DonViQuyDoi 	and ((tk.ID_DonVi = @ID_ChiNhanh and hh.LaHangHoa='1') or hh.LaHangHoa=0)
	left join DM_GiaVon gv on qd.id= gv.ID_DonViQuiDoi and ((gv.ID_DonVi= @ID_ChiNhanh) or gv.ID_DonVi is null )
	left join (select qd2.ID,sum(dl.SoLuong *  ISNULL(gv.GiaVon,0)) as GiaVon
				from DonViQuiDoi qd2
				join DinhLuongDichVu dl on qd2.ID= dl.ID_DichVu
				left join DM_GiaVon gv on dl.ID_DonViQuiDoi= gv.ID_DonViQuiDoi
				where gv.ID_DonVi=@ID_ChiNhanh 
				group by qd2.ID
				) tblDVu on qd.ID= tblDVu.ID
	where qd.Xoa= 0 and hh.TheoDoi=1	
	and	(
		(select count(*) from @tablename b where 
    		qd.MaHangHoa like '%'+b.Name+'%' 
			or hh.TenHangHoa like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 			    		
			)=@count or @count=0
			)  
	and	(
		(select count(*) from @tablenameChar b where 
    		qd.MaHangHoa like '%'+b.Name+'%' 
			or hh.TenHangHoa like '%'+b.Name+'%' 
    		or hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 			    		
			)=@countChar or @countChar=0
			) 
	order by tk.TonKho desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[SMS_KhachHangGiaoDich]
    @ID_ChiNhanh [nvarchar](max),
    @ID_NhomKhachHang [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @Where [nvarchar](max),
    @CurrentPage [int],
    @PageSize [float]
AS
BEGIN
    SET NOCOUNT ON;
    	if @Where='' set @Where= ''
    else set @Where= ' AND '+ @Where 
    
    	declare @from int= @CurrentPage * @PageSize + 1  
    declare @to int= (@CurrentPage + 1) * @PageSize 
    
    	declare @sql1 nvarchar(max)= concat('
    	declare @tblIDNhoms table (ID varchar(36)) 
    	declare @idNhomDT nvarchar(max) = ''', @ID_NhomKhachHang, '''
    
    	declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh select * from splitstring(''',@ID_ChiNhanh, ''')
    	
    	if @idNhomDT =''%%''
    		begin
    				insert into @tblIDNhoms(ID) values (''00000000-0000-0000-0000-000000000000'')
    
    			-- check QuanLyKHTheochiNhanh
    			--declare @QLTheoCN bit = (select QuanLyKhachHangTheoDonVi from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID))  
    				
    				declare @QLTheoCN bit = 0;
    				declare @countQL int=0;
    				select distinct QuanLyKhachHangTheoDonVi into #temp from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID)
    				set @countQL = (select COUNT(*) from #temp)
    				if	@countQL= 1 
    						set @QLTheoCN = (select QuanLyKhachHangTheoDonVi from #temp)
    				
    			if @QLTheoCN = 1
    				begin									
    					insert into @tblIDNhoms(ID)
    					select *  from (
    						-- get Nhom not not exist in NhomDoiTuong_DonVi
    						select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    						where not exists (select ID_NhomDoiTuong from NhomDoiTuong_DonVi where ID_NhomDoiTuong= nhom.ID) 
    						and LoaiDoiTuong = 1
    						union all
    						-- get Nhom at this ChiNhanh
    						select convert(varchar(36),ID_NhomDoiTuong)  from NhomDoiTuong_DonVi where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) ) tbl
    				end
    			else
    				begin				
    				-- insert all
    				insert into @tblIDNhoms(ID)
    				select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    				where LoaiDoiTuong = 1
    				end		
    		end
    	else
    		begin
    			set @idNhomDT = REPLACE( @idNhomDT,''%'','''')
    			insert into @tblIDNhoms(ID) values (@idNhomDT)
    		end',
    		';'
    		)
    
    	declare @sql2 nvarchar(max)= 
    	concat(' WITH Data_CTE ',
    		' AS ',
    		' ( ',
    				N'SELECT  *
    			 FROM
    			 (
    						select hd.MaHoaDon, hd.LoaiHoaDon, hd.NgayLapHoaDon, 
    							dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.TenNhomDoiTuongs as TenNhomDT,
    							case when dt.IDNhomDoiTuongs='''' then ''00000000-0000-0000-0000-000000000000''
								else  ISNULL(dt.IDNhomDoiTuongs,''00000000-0000-0000-0000-000000000000'') end as ID_NhomDoiTuong,
    							sms.ThoiGianGui,
								sms.NoiDung,
    							ISNULL(sms.TrangThai,999) as TrangThai,
								case when sms.TrangThai is null then N''Chưa gửi''
									else iif(sms.TrangThai = 100, N''Gửi thành công'',N''Gửi thất bại'') end as STrangThai,
    							nd.TaiKhoan as NguoiGui							
    						from BH_HoaDon hd
    						join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
    						left join HeThong_SMS sms on hd.ID= sms.ID_HoaDon
    						left join HT_NguoiDung nd on sms.ID_NguoiGui= nd.ID
    						where hd.LoaiHoaDon in (1,3,19,25)
    						and hd.ID_DoiTuong !=''00000000-0000-0000-0000-000000000000''
    						and hd.ChoThanhToan is not null
    						and ISNULL(dt.DienThoai,'''')!=''''
    						and hd.NgayLapHoaDon >''', @FromDate,''' and hd.NgayLapHoaDon < ''',@ToDate, '''', @Where,
    				  ')b
    				  WHERE 
    			   EXISTS(SELECT Name FROM splitstring(b.ID_NhomDoiTuong) lstFromtbl inner JOIN @tblIDNhoms tblsearch ON lstFromtbl.Name = tblsearch.ID)
    			),
    			Count_CTE ',
    			' AS ',
    			' ( ',
    			' SELECT COUNT(*) AS TotalRow, CEILING(COUNT(*) / CAST(',@PageSize, ' as float )) as TotalPage FROM Data_CTE ',
    			' ) ',
    				' SELECT dt.*, cte.TotalPage, cte.TotalRow
    				  FROM Data_CTE dt
    			  CROSS JOIN Count_CTE cte
    				  ORDER BY NgayLapHoaDon desc',		 
    				' OFFSET (', @CurrentPage, ' * ', @PageSize ,') ROWS ',
    			' FETCH NEXT ', @PageSize , ' ROWS ONLY ')
    
    				print (@sql2)
    		exec (@sql1 + @sql2 )
END");

			Sql(@"ALTER PROCEDURE [dbo].[SMS_KhachHangSinhNhat]
    @ID_ChiNhanh [nvarchar](max),
    @ID_NhomKhachHang [nvarchar](max),
    @Where [nvarchar](max),
    @CurrentPage [int],
    @PageSize [float]
AS
BEGIN
    SET NOCOUNT ON;
    	if @Where='' set @Where= ''
    else set @Where= ' AND '+ @Where 
    
    	declare @from int= @CurrentPage * @PageSize + 1  
    declare @to int= (@CurrentPage + 1) * @PageSize 
    
    	declare @sql1 nvarchar(max)= concat('
    	declare @tblIDNhoms table (ID varchar(36)) 
    	declare @idNhomDT nvarchar(max) = ''', @ID_NhomKhachHang, '''
    
    	declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh select * from splitstring(''',@ID_ChiNhanh, ''')
    	
    	if @idNhomDT =''%%''
    		begin
    				insert into @tblIDNhoms(ID) values (''00000000-0000-0000-0000-000000000000'')
    
    			-- check QuanLyKHTheochiNhanh
    			--declare @QLTheoCN bit = (select QuanLyKhachHangTheoDonVi from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID))  
    				
    				declare @QLTheoCN bit = 0;
    				declare @countQL int=0;
    				select distinct QuanLyKhachHangTheoDonVi into #temp from HT_CauHinhPhanMem where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID)
    				set @countQL = (select COUNT(*) from #temp)
    				if	@countQL= 1 
    						set @QLTheoCN = (select QuanLyKhachHangTheoDonVi from #temp)
    				
    			if @QLTheoCN = 1
    				begin									
    					insert into @tblIDNhoms(ID)
    					select *  from (
    						-- get Nhom not not exist in NhomDoiTuong_DonVi
    						select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    						where not exists (select ID_NhomDoiTuong from NhomDoiTuong_DonVi where ID_NhomDoiTuong= nhom.ID) 
    						and LoaiDoiTuong = 1
    						union all
    						-- get Nhom at this ChiNhanh
    						select convert(varchar(36),ID_NhomDoiTuong)  from NhomDoiTuong_DonVi where exists (select * from @tblChiNhanh cn where ID_DonVi= cn.ID) ) tbl
    				end
    			else
    				begin				
    				-- insert all
    				insert into @tblIDNhoms(ID)
    				select convert(varchar(36),ID) as ID_NhomDoiTuong from DM_NhomDoiTuong nhom  
    				where LoaiDoiTuong = 1
    				end		
    		end
    	else
    		begin
    			set @idNhomDT = REPLACE( @idNhomDT,''%'','''')
    			insert into @tblIDNhoms(ID) values (@idNhomDT)
    		end',
    		';'
    		)
    
    	declare @sql2 nvarchar(max)= 
    	concat(N' WITH Data_CTE ',
    		' AS ',
    		' ( ',
    				N'SELECT  *
    			 FROM
    			 (
    						select dt.ID,
    							dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.TenNhomDoiTuongs as TenNhomDT,dt.NgaySinh_NgayTLap,
    							case when dt.IDNhomDoiTuongs='''' then ''00000000-0000-0000-0000-000000000000'' else  ISNULL(dt.IDNhomDoiTuongs,''00000000-0000-0000-0000-000000000000'') end as ID_NhomDoiTuong,
    							sms.ThoiGianGui,
    							ISNULL(sms.NoiDung,'''') as NoiDung,
    							ISNULL(sms.TrangThai,999) as TrangThai,
								case when sms.TrangThai is null then N''Chưa gửi''
									else iif(sms.TrangThai = 100, N''Gửi thành công'',N''Gửi thất bại'') end as STrangThai,
    							nd.TaiKhoan	as NguoiGui					
    						from DM_DoiTuong dt
    						left join HeThong_SMS sms on dt.ID= sms.ID_KhachHang
    						left join HT_NguoiDung nd on sms.ID_NguoiGui= nd.ID
    						where dt.NgaySinh_NgayTLap is not null and ISNULL(dt.DienThoai,'''') !=''''
    						and dt.TheoDoi=0 and dt.LoaiDoiTuong = 1 ',@Where,						
    				  ')b
    				  WHERE 
    			   EXISTS(SELECT Name FROM splitstring(b.ID_NhomDoiTuong) lstFromtbl inner JOIN @tblIDNhoms tblsearch ON lstFromtbl.Name = tblsearch.ID)
    			),
    			Count_CTE ',
    			' AS ',
    			' ( ',
    			' SELECT COUNT(*) AS TotalRow, CEILING(COUNT(*) / CAST(',@PageSize, ' as float )) as TotalPage FROM Data_CTE ',
    			' ) ',
    				' SELECT dt.*, cte.TotalPage, cte.TotalRow
    				  FROM Data_CTE dt
    			  CROSS JOIN Count_CTE cte
    				  ORDER BY NgaySinh_NgayTLap',		 
    				' OFFSET (', @CurrentPage, ' * ', @PageSize ,') ROWS ',
    			' FETCH NEXT ', @PageSize , ' ROWS ONLY ')
    			--	print (@sql2)
    		exec (@sql1 + @sql2 )
END");

			Sql(@"ALTER PROCEDURE [dbo].[SP_GetQuyen_ByIDNguoiDung]
    @ID_NguoiDung [nvarchar](max),
    @ID_DonVi [nvarchar](max)
AS
BEGIN
    DECLARE @LaAdmin bit
    
    	select top 1 @LaAdmin =  LaAdmin from HT_NguoiDung where ID like @ID_NguoiDung
    
    	-- LaAdmin: full quyen, assign ID = neID() --> because class HT_Quyen_NhomDTO {ID, MaQuyen}
    	if @LaAdmin	='1'
    		select NEWID() as ID,  MaQuyen from HT_Quyen where DuocSuDung = '1'
    	else	
    		select NEWID() as  ID, MaQuyen 
    		from HT_NguoiDung_Nhom nnd
    		JOIN HT_Quyen_Nhom qn on nnd.IDNhomNguoiDung = qn.ID_NhomNguoiDung
    		where nnd.IDNguoiDung like @ID_NguoiDung and nnd.ID_DonVi like @ID_DonVi
END");

			Sql(@"ALTER PROCEDURE [dbo].[ValueCard_GetListHisUsed]
	@ID_ChiNhanhs [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
    @ID_KhachHang [nvarchar](max)='451860c1-c84b-42ab-ba0e-ff8505ee2907',
    @DateFrom datetime='2016-01-01',
    @DateTo datetime='2022-12-20',
	@CurrentPage int=0,
	@PageSize int=50
AS
BEGIN
    SET NOCOUNT ON;
	set @DateTo = DATEADD(day, 1, @DateTo)

	select 
		ID,
		LoaiHoaDon,
		MaHoaDon,
		NgayLapHoaDon,			
		DienGiai,
		iif(LoaiHoaDon = 6 or (LoaiHoaDon = 23 and TongTienHang > 0), TongTienHang,0) as PhatSinhTang,
		iif(LoaiHoaDon != 6 or (LoaiHoaDon = 23 and TongTienHang < 0), abs(TongTienHang) ,0) as PhatSinhGiam,
		TongTienHang,	
		ROW_NUMBER() over ( order by NgayLapHoaDon) as RN,
		cast(0 as float) as SoDuTruocPhatSinh,
		sum(TongTienHang) over (order by NgayLapHoaDon) as SoDuSauPhatSinh
	into #tblHis
	from
	(
	------ napthe ----
	select  
		hd.ID,
		hd.LoaiHoaDon,
		hd.MaHoaDon,
		hd.NgayLapHoaDon,
		hd.TongTienHang,
		hd.DienGiai
	from BH_HoaDon hd
	where hd.ChoThanhToan='0'
	and hd.ID_DoiTuong = @ID_KhachHang
	and hd.LoaiHoaDon= 22

	union all
		------  dieuchinh the ----
	select  
		hd.ID,
		hd.LoaiHoaDon,
		hd.MaHoaDon,
		hd.NgayLapHoaDon,
		hd.TongTienHang,
		hd.DienGiai
	from BH_HoaDon hd
	where hd.ChoThanhToan='0'
	and hd.ID_DoiTuong = @ID_KhachHang
	and hd.LoaiHoaDon= 23

	union all
	------  hoan sodu  ----
	select  
		hd.ID,
		hd.LoaiHoaDon,
		hd.MaHoaDon,
		hd.NgayLapHoaDon, 
		-hd.TongTienHang as GiaTriHoanTra,
		hd.DienGiai
	from BH_HoaDon hd
	where hd.ChoThanhToan='0'
	and hd.ID_DoiTuong = @ID_KhachHang
	and hd.LoaiHoaDon= 32

	union all
	------  sudung the---
	select	
		qhd.ID, --- get idQuyHoadon (used to group by)
		hd.LoaiHoaDon,
		hd.MaHoaDon,
		qhd.NgayLapHoaDon,		
		-qct.TienThu as GiaTriSuDung,
		qhd.NoiDungThu
	from Quy_HoaDon qhd
	join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
	left join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
	where qct.ID_DoiTuong= @ID_KhachHang
	and qct.HinhThucThanhToan=4
	and qhd.LoaiHoaDon = 11
	and (qhd.TrangThai is null or qhd.TrangThai= 1)
	and qhd.NgayLapHoaDon between @DateFrom and @DateTo

	union all
	------ trahang (tang sodu) ---
	select	
		hd.ID,
		hd.LoaiHoaDon,
		hd.MaHoaDon,
		hd.NgayLapHoaDon,		
		qct.TienThu as HoanTraThe,
		qhd.NoiDungThu
	from Quy_HoaDon qhd
	join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
	join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
	where qct.ID_DoiTuong= @ID_KhachHang
	and qct.HinhThucThanhToan=4
	and qhd.LoaiHoaDon = 12
	and (qhd.TrangThai is null or qhd.TrangThai= 1)
	and qhd.NgayLapHoaDon between @DateFrom and @DateTo
	) tblU
	order by NgayLapHoaDon 


	---- update sodu truoc phat sinh ---
	update tbl set tbl.SoDuTruocPhatSinh= tbl2.SoDuSauPhatSinh
	from #tblHis tbl
	join #tblHis tbl2 on tbl.RN = tbl2.RN + 1




		;with data_cte
		as
		(
	   select tbl.ID,
			tbl.NgayLapHoaDon,	
			sum(tbl.PhatSinhTang)  as PhatSinhTang,					
			max(tbl.SoDuTruocPhatSinh) as SoDuTruoc,
			sum(tbl.PhatSinhGiam)  as PhatSinhGiam,
			max(tbl.SoDuSauPhatSinh) as SoDuSau,
			tblMa.MaHoaDons as MaHoaDon,
			tblLoai.LoaiHoaDons as SLoaiHoaDon,
			tbl.DienGiai
	   from #tblHis tbl
	   join (    	
	   ------ merger text MaHoaDon to 1 row
    		Select distinct tblXML.ID, 
    					(
    			select distinct hd.MaHoaDon +', '  AS [text()]
    			from #tblHis hd    			
    			where hd.ID= tblXML.ID
    			For XML PATH ('')
    		) MaHoaDons
    	from #tblHis tblXML
	 ) tblMa on tblMa.ID= tbl.ID
	  join (    	
	   ------ merger LoaiHoaDon
    		Select distinct tblXML.ID, 
    					(
    			select distinct 
					(case hd.LoaiHoaDon
						when 1 then N'Bán hàng' 
						when 3 then N'Đặt hàng' 
						when 6 then N'Trả hàng' 
						when 19 then N'Gói dịch vụ' 
						when 22 then N'Nạp thẻ' 
						when 23 then N'Điều chỉnh thẻ' 
						else '' end )
					+', '  AS [text()]
    			from #tblHis hd    			
    			where hd.ID= tblXML.ID
    			For XML PATH ('')
    		) LoaiHoaDons
    	from #tblHis tblXML
	 ) tblLoai on tblLoai.ID= tbl.ID
	 where tbl.LoaiHoaDon!=22
	 group by tbl.ID,
		tbl.NgayLapHoaDon,
		tbl.DienGiai,
		tblLoai.LoaiHoaDons, tblMa.MaHoaDons	
	),
	count_cte as
	(
		select COUNT(ID) as TotalRow ,
			 ceiling(COUNT(ID) /cast(@PageSize as float)) as TotalPage,
			 sum(PhatSinhTang) as TongPhatSinhTang,
			 sum(PhatSinhGiam) as TongPhatSinhGiam
		from data_cte
	)
	select *
	from data_cte
	cross join count_cte
	order by NgayLapHoaDon desc
	offset (@CurrentPage * @PageSize) rows
	fetch next @PageSize rows only
   

END");
        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[LoadDanhMuc_KhachHangNhaCungCap]");
			DropStoredProcedure("[dbo].[GetHangCungLoai_byID]");
        }
    }
}
