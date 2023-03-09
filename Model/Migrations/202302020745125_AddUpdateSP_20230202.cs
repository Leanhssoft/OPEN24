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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_ChiTiet_Page]
    @pageNumber [int],
    @pageSize [int],
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
	@LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang UNIQUEIDENTIFIER,
	@LoaiChungTu [nvarchar](max),
	@HanBaoHanh [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN
	set nocount on;
	---- bo sung timkiem NVthuchien
	set @pageNumber = @pageNumber -1;

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER)
	INSERT INTO @tblChiNhanh
	select Name from splitstring(@ID_ChiNhanh);

	DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    		Case when nd.LaAdmin = '1' then '1' else
    		Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    		From
    		HT_NguoiDung nd	
    		where nd.ID = @ID_NguoiDung)	

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
		GiaVon float,
		TienVon float
		)

	insert into @tblCTHD
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu
			
		select *
		into #tblView
		from
		(
		select 
			c.ID_ChiTietHD,
			hh.ID, hh.TenHangHoa,
			qd.MaHangHoa,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			concat(hh.TenHangHoa, qd.ThuocTinhGiaTri) as TenHangHoaFull,
			qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			ISNULL(nhh.TenNhomHangHoa,  N'Nhóm hàng hóa mặc định') as TenNhomHangHoa,
			lo.MaLoHang as TenLoHang,
			qd.TenDonViTinh,
			cast(c.SoLuong as float) as SoLuong,
			cast(c.DonGia as float) as GiaBan,
			cast(c.TienChietKhau as float) as TienChietKhau,
			cast(c.ThanhTienTruocCK as float) as ThanhTienTruocCK,
			cast(c.ThanhTien as float) as ThanhTien,
			cast(c.GiamGiaHD as float) as GiamGiaHD,
			cast(c.TienThue as float) as TienThue,
			iif(@XemGiaVon='1',cast(c.GiaVon as float),0) as GiaVon,
			iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
			cast(c.ThanhTien - c.GiamGiaHD as float) as DoanhThu,
			iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.TienVon -c.ChiPhi as float),0) as LaiLo,
			c.NgayLapHoaDon, c.MaHoaDon, c.ID_PhieuTiepNhan, c.ID_DoiTuong, c.ID_NhanVien,
			c.ThoiGianBaoHanh, c.HanBaoHanh, c.TrangThai, c.GhiChu,
			dt.MaDoiTuong as MaKhachHang, 
			dt.TenDoiTuong as TenKhachHang, 
			dt.TenNhomDoiTuongs as NhomKhachHang, 
			dt.DienThoai, dt.GioiTinhNam,
			dt.ID_NguoiGioiThieu, dt.ID_NguonKhach,
			--c.TenNhanVien,
			c.ChiPhi,
			c.LoaiHoaDon,
			iif(c.TenHangHoaThayThe is null or c.TenHangHoaThayThe='', hh.TenHangHoa, c.TenHangHoaThayThe) as TenHangHoaThayThe			
		from 
		(
		select 
			b.ID as ID_ChiTietHD,
			b.LoaiHoaDon,b.NgayLapHoaDon, b.MaHoaDon, b.ID_PhieuTiepNhan, b.ID_DoiTuong, b.ID_NhanVien, 
			---b.TenNhanVien,
			b.ThoiGianBaoHanh, b.HanBaoHanh, b.TrangThai, b.GhiChu,
			b.SoLuong * isnull(qd.TyLeChuyenDoi,1) as SoLuong,
			b.ThanhTien,
			b.GiaVon,
			b.TienVon,		
			qd.ID_HangHoa,
			b.ID_LoHang,	
			b.GiamGiaHD,
			b.TienThue,					
			b.DonGia,		
			b.TienChietKhau,----- tong ck moi mathang
			b.ThanhTienTruocCK,
			b.ChiPhi,
			b.TenHangHoaThayThe
		from (
		select 
			ct.ID,
			ct.LoaiHoaDon,ct.NgayLapHoaDon, ct.MaHoaDon, ct.ID_PhieuTiepNhan, ct.ID_DoiTuong, ct.ID_NhanVien,	
			--nvien.NVThucHien as TenNhanVien,
			ct.TienThue,
			ct.GiamGiaHD,			
			
			ct.ID_DonViQuiDoi, 
			ct.ID_LoHang,
			ct.TenHangHoaThayThe,
			case ct.LoaiThoiGianBH
				when 1 then CONVERT(varchar(100), ct.ThoiGianBaoHanh) + N' ngày'
				when 2 then CONVERT(varchar(100), ct.ThoiGianBaoHanh) + ' tháng'
				when 3 then CONVERT(varchar(100), ct.ThoiGianBaoHanh) + ' năm'
			else '' end as ThoiGianBaoHanh,
			case ct.LoaiThoiGianBH
				when 1 then DATEADD(DAY, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)
				when 2 then DATEADD(MONTH, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)
				when 3 then DATEADD(YEAR, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)
			end as HanBaoHanh,
			Case when ct.LoaiThoiGianBH = 1 and DATEADD(DAY, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)  < GETDATE() then N'Hết hạn'
			when ct.LoaiThoiGianBH = 2 and DATEADD(MONTH, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)  < GETDATE() then N'Hết hạn'
			when ct.LoaiThoiGianBH = 3 and DATEADD(YEAR, ct.ThoiGianBaoHanh, ct.NgayLapHoaDon)  < GETDATE() then N'Hết hạn'
			when ct.LoaiThoiGianBH in (1,2,3) Then N'Còn hạn'
			else '' end as TrangThai,
			ct.GhiChu,
			ct.DonGia,		
			ct.SoLuong * ct.TienChietKhau as TienChietKhau,
			ct.SoLuong * ct.DonGia as ThanhTienTruocCK,
			ct.SoLuong,
			ct.ThanhTien,
			iif(ct.SoLuong =0, 0, ct.TienVon/ct.SoLuong) as GiaVon,			
			ct.TienVon,
			isnull(cp.ChiPhi,0) as ChiPhi
	from @tblCTHD ct	
	left join @tblChiPhi cp on ct.ID= cp.ID_ParentCombo
	--left join
	--	(
	--	-- get nvthuchien of hdbl
	--		select distinct th.ID_ChiTietHoaDon ,
	--			 (
	--					select nv.TenNhanVien +', '  AS [text()]
	--					from BH_NhanVienThucHien nvth
	--					join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID
	--					join BH_HoaDon_ChiTiet ct on nvth.ID_ChiTietHoaDon = ct.ID
	--					join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
	--					where nvth.ID_ChiTietHoaDon = th.ID_ChiTietHoaDon
	--					and (hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd) 
 --   					and hd.ChoThanhToan = 0 
	--					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)						
	--					For XML PATH ('')
	--				) NVThucHien
	--			from BH_NhanVienThucHien th 
	--	) nvien on ct.ID = nvien.ID_ChiTietHoaDon
		where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)	
		)b
		join DonViQuiDoi qd on b.ID_DonViQuiDoi= qd.ID		
		) c
		join DM_HangHoa hh on c.ID_HangHoa = hh.ID
		join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan=1
		left join DM_LoHang lo on c.ID_LoHang = lo.ID
		left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
		left join DM_DoiTuong dt on c.ID_DoiTuong = dt.ID		
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)	
    	and hh.TheoDoi like @TheoDoi
		and qd.Xoa like @TrangThai
		and c.TrangThai like @HanBaoHanh		
		AND
		((select count(Name) from @tblSearchString b where 
				c.MaHoaDon like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or hh.TenHangHoa like '%'+b.Name+'%'
    				or lo.MaLoHang like '%' +b.Name +'%' 
    			or qd.MaHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    				or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    				or qd.TenDonViTinh like '%'+b.Name+'%'
					or dt.TenDoiTuong like '%'+b.Name+'%'
    				or dt.TenDoiTuong_KhongDau  like '%'+b.Name+'%'
					or dt.MaDoiTuong like '%'+b.Name+'%'
    				or dt.DienThoai  like '%'+b.Name+'%'
					or c.GhiChu like N'%'+b.Name+'%'
				--	or c.TenNhanVien like N'%'+b.Name+'%'
					or c.GhiChu like N'%'+b.Name+'%'
    				or qd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
		)a where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))	
		
		
	
			DECLARE @Rows FLOAT,  @TongSoLuong float, 
			@TongChietKhau float,@ThanhTienTruocCK float,
			@TongThanhTien float, @TongGiamGiaHD FLOAT, @TongTienVon FLOAT, 
			@TongLaiLo FLOAT, @SumTienThue FLOAT,@TongDoanhThuThuan FLOAT, @TongChiPhi float			
			SELECT @Rows = Count(*), @TongSoLuong = SUM(SoLuong),
			@TongChietKhau= SUM(TienChietKhau),
			@ThanhTienTruocCK= SUM(ThanhTienTruocCK),
			@TongThanhTien = SUM(ThanhTien), @TongGiamGiaHD = SUM(GiamGiaHD),
			@TongTienVon = SUM(TienVon), @TongLaiLo = SUM(LaiLo), @SumTienThue = SUM(TienThue),
			@TongDoanhThuThuan = SUM(DoanhThu),
			@TongChiPhi = SUM(ChiPhi) 
			FROM #tblView;

			select 
				tbl.*,
				nvien.NVThucHien as TenNhanVien,
				ISNULL(nk.TenNguonKhach,'') as TenNguonKhach,
				isnull(gt.TenDoiTuong,'') as NguoiGioiThieu				
			from(
				select *,							
					@Rows as Rowns,
    				@TongSoLuong as TongSoLuong,
					@TongChietKhau as TongChietKhau,
					@ThanhTienTruocCK as TongTienTruocCK,
    				@TongThanhTien as TongThanhTien,
    				@TongGiamGiaHD as TongGiamGiaHD,
    				@TongTienVon as TongTienVon,
    				@TongLaiLo as TongLaiLo,
					@SumTienThue as TongTienThue,
    				@TongDoanhThuThuan as DoanhThuThuan,
    				@TongChiPhi as TongChiPhi
    			from #tblView tbl
				order by NgayLapHoaDon DESC
				OFFSET (@pageNumber* @pageSize) ROWS
    			FETCH NEXT @pageSize ROWS ONLY	
			) tbl
			left join DM_NguonKhachHang nk on tbl.ID_NguonKhach= nk.ID
			left join DM_DoiTuong gt on tbl.ID_NguoiGioiThieu= gt.ID 	   
			left join
				(
				-- get nvthuchien of hdbl
				select distinct th.ID_ChiTietHoaDon ,
					 (
							select nv.TenNhanVien +', '  AS [text()]
							from BH_NhanVienThucHien nvth
							join NS_NhanVien nv on nvth.ID_NhanVien = nv.ID		
							where nvth.ID_ChiTietHoaDon = th.ID_ChiTietHoaDon
							For XML PATH ('')
						) NVThucHien
					from BH_NhanVienThucHien th 
			) nvien on tbl.ID_ChiTietHD = nvien.ID_ChiTietHoaDon
			order by NgayLapHoaDon desc
END");

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
    	GiamGia FLOAT, DoanhThu FLOAT,
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
		IIF(hd.TongTienHang =0,hdct.ThanhTien,(hdct.ThanhTien * (1 - hd.TongGiamGia/hd.TongTienHang))),0) AS DoanhThu,
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
    	GiamGia FLOAT, DoanhThu FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), TienVon FLOAT, LoiNhuan FLOAT, ChiPhi FLOAT)
    
    	INSERT INTO @tblBaoCaoDoanhThu
		SELECT bcsc.MaPhieuTiepNhan, bcsc.NgayVaoXuong, bcsc.BienSo, bcsc.IDChiTiet,
    	bcsc.MaDoiTuong, bcsc.TenDoiTuong, bcsc.CoVanDichVu,
    	bcsc.ID, bcsc.MaHoaDon, bcsc.NgayLapHoaDon,
    	bcsc.MaHangHoa, bcsc.TenHangHoa, bcsc.TenDonViTinh, bcsc.SoLuong, bcsc.DonGia, bcsc.TienChietKhau, bcsc.ThanhTien, bcsc.TienThue,
    	bcsc.GiamGia, bcsc.DoanhThu,
    	bcsc.GhiChu, bcsc.MaDonVi, bcsc.TenDonVi, SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS TienVon,
    	bcsc.DoanhThu - SUM(ISNULL(bcsc.GiaVon,0)*ISNULL(bcsc.SoLuongxk,0)) AS LoiNhuan, 0
		FROM
    	(
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, hdsc.IDChiTiet, hdsc.ID_ChiTietDinhLuong,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	hdsc.MaHangHoa, hdsc.TenHangHoa, hdsc.TenDonViTinh, hdsc.SoLuong, hdsc.DonGia, hdsc.TienChietKhau, hdsc.ThanhTien, hdsc.TienThue,
    	hdsc.GiamGia, hdsc.DoanhThu,
    	hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon 
		AND xkct.ID_ChiTietGoiDV = hdsc.IDChiTiet
    	WHERE xk.LoaiHoaDon = 8 OR xk.ID IS NULL 
		--AND (hdsc.IDChiTiet = hdsc.ID_ChiTietDinhLuong OR hdsc.ID_ChiTietDinhLuong IS NULL)
		UNION ALL
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, NULL, null,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon,
    	dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, 0 AS SoLuong, 0 AS DonGia, 0 AS TienChietKhau, 0 AS ThanhTien, 0 AS TienThue,
    	0 AS GiamGia, 0 AS DoanhThu,
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
    	0, 0,
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
    	bcsc.GiamGia, bcsc.DoanhThu,
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

    	DECLARE @SThanhTien FLOAT,  @SChietKhau FLOAT, @SThue FLOAT, @SGiamGia FLOAT, @SDoanhThu FLOAT, @STongTienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT;
    	SELECT @SThanhTien = SUM(ThanhTien), @SChietKhau = SUM(TienChietKhau), @SThue = SUM(TienThue), @SGiamGia = SUM(GiamGia), 
		@SDoanhThu = SUM(DoanhThu), @STongTienVon = SUM(TienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi)
    	FROM @tblBaoCaoDoanhThu
    
    	SELECT IDChiTiet, MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu , ID AS IDHoaDon, MaHoaDon,
    	NgayLapHoaDon, MaHangHoa, TenHangHoa, TenDonViTinh, ISNULL(SoLuong, 0) AS SoLuong, ISNULL(DonGia, 0) AS DonGia, ISNULL(TienChietKhau, 0) AS TienChietKhau, 
    	ISNULL(TienThue,0) AS TienThue, ISNULL(ThanhTien,0) AS ThanhTien, ISNULL(GiamGia, 0) AS GiamGia, ISNULL(DoanhThu, 0) AS DoanhThu, ISNULL(TienVon,0) AS TienVon, ISNULL(LoiNhuan,0) AS LoiNhuan,
    	GhiChu, MaDonVi, TenDonVi, ChiPhi, ISNULL(@SThanhTien, 0) AS SThanhTien, ISNULL(@SChietKhau,0) AS SChietKhau,
    	ISNULL(@SThue,0) AS SThue, ISNULL(@SGiamGia,0) AS SGiamGia, ISNULL(@SDoanhThu, 0) AS SDoanhThu, ISNULL(@STongTienVon,0) AS STongTienVon,
    	ISNULL(@SLoiNhuan,0) AS SLoiNhuan, ISNULL(@SChiPhi, 0) AS SChiPhi
    	FROM @tblBaoCaoDoanhThu
    	WHERE (@DoanhThuFrom IS NULL OR DoanhThu >= @DoanhThuFrom)
    	AND (@DoanhThuTo IS NULL OR DoanhThu <= @DoanhThuTo)
    	AND (@LoiNhuanFrom IS NULL OR LoiNhuan >= @LoiNhuanFrom)
    	AND (@LoiNhuanTo IS NULL OR LoiNhuan <= @LoiNhuanTo)
    	ORDER BY NgayLapHoaDon
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
    	IDPhieuTiepNhan UNIQUEIDENTIFIER, IDHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, DoanhThu FLOAT, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT nv.ID, nv.MaNhanVien, nv.TenNhanVien, ptn.ID, hd.ID, hd.NgayLapHoaDon, hd.TongThanhToan - hd.TongTienThue, dv.MaDonVi, dv.TenDonVi
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
    		LEFT JOIN BH_HoaDon xk ON hdsc.IDHoaDon = xk.ID_HoaDon
    		LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    		WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
			UNION ALL
			SELECT hdsc.IDCoVan, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    		FROM (SELECT IDCoVan, IDPhieuTiepNhan FROM @tblHoaDonSuaChua GROUP BY IDCoVan, IDPhieuTiepNhan ) hdsc
    		INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan
    		INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    		WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND xk.ID_HoaDon IS NULL
		) hdsc
    	GROUP BY hdsc.IDCoVan
    
    	DECLARE @SSoLanTiepNhan FLOAT, @SSoLuongHoaDon FLOAT, @STongDoanhThu FLOAT, @STienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(IDCoVan UNIQUEIDENTIFIER, MaNhanVien NVARCHAR(MAX), TenNhanVien NVARCHAR(MAX),
    	SoLanTiepNhan FLOAT, SoLuongHoaDon FLOAT, TongDoanhThu FLOAT, TongTienVon FLOAT, LoiNhuan FLOAT, NgayGiaoDichGanNhat DATETIME, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT)
    	
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hd.IDCoVan, hd.MaNhanVien, hd.TenNhanVien, hd.SoLanTiepNhan, hd.SoLuongHoaDon,
    	ISNULL(hd.TongDoanhThu,0) AS TongDoanhThu, ISNULL(tv.TienVon,0) AS TongTienVon, ISNULL(hd.TongDoanhThu,0) - ISNULL(tv.TienVon,0) AS LoiNhuan, hd.NgayGiaoDichGanNhat, hd.MaDonVi, hd.TenDonVi, 0
    	FROM
    	(
    	SELECT IDCoVan, MaNhanVien, TenNhanVien, MaDonVi, TenDonVi, COUNT(DISTINCT IDPhieuTiepNhan) AS SoLanTiepNhan, COUNT(IDHoaDon) AS SoLuongHoaDon, SUM(DoanhThu) AS TongDoanhThu,
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

    	SELECT @SSoLanTiepNhan = SUM(SoLanTiepNhan), @SSoLuongHoaDon = SUM(SoLuongHoaDon), @STongDoanhThu = SUM(TongDoanhThu), @STienVon = SUM(TongTienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi) FROM @tblBaoCaoDoanhThu
    
    	SELECT *, CAST(@SSoLanTiepNhan AS FLOAT) AS SSoLanTiepNhan, @SSoLuongHoaDon AS SSoLuongHoaDon, @STongDoanhThu AS STongDoanhThu, @STienVon AS STienVon, @SLoiNhuan AS SLoiNhuan, @SChiPhi AS SChiPhi FROM @tblBaoCaoDoanhThu
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
    	IDPhieuTiepNhan UNIQUEIDENTIFIER, IDHoaDon UNIQUEIDENTIFIER, NgayLapHoaDon DATETIME, DoanhThu FLOAT, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT dmx.ID, dmx.BienSo, dmx.SoMay, dmx.SoKhung,
    	dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, ptn.ID, hd.ID, hd.NgayLapHoaDon, hd.TongThanhToan - hd.TongTienThue, dv.MaDonVi, dv.TenDonVi
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
    	LEFT JOIN BH_HoaDon xk ON hdsc.IDHoaDon = xk.ID_HoaDon
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
		UNION ALL
		SELECT hdsc.IDXe, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM (SELECT IDPhieuTiepNhan, IDXe FROM @tblHoaDonSuaChua GROUP BY IDPhieuTiepNhan, IDXe) hdsc
    	INNER JOIN BH_HoaDon xk ON hdsc.IDPhieuTiepNhan = xk.ID_PhieuTiepNhan
    	INNER JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) AND xk.ID_HoaDon IS NULL
		) hdsc
    	GROUP BY hdsc.IDXe;
    
    	DECLARE @SSoLanTiepNhan FLOAT, @SSoLuongHoaDon FLOAT, @STongDoanhThu FLOAT, @STienVon FLOAT, @SLoiNhuan FLOAT, @SChiPhi FLOAT;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(IDXe UNIQUEIDENTIFIER, BienSo NVARCHAR(MAX), SoKhung NVARCHAR(MAX), SoMay NVARCHAR(MAX), MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX),
    	DienThoai NVARCHAR(MAX), SoLanTiepNhan FLOAT, SoLuongHoaDon FLOAT, TongDoanhThu FLOAT, TongTienVon FLOAT, LoiNhuan FLOAT, NgayGiaoDichGanNhat DATETIME, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), ChiPhi FLOAT)
    	
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hd.IDXe, hd.BienSo, hd.SoKhung, hd.SoMay, hd.MaDoiTuong, hd.TenDoiTuong, hd.DienThoai, hd.SoLanTiepNhan, hd.SoLuongHoaDon,
    	ISNULL(hd.TongDoanhThu,0) AS TongDoanhThu, ISNULL(tv.TienVon,0) AS TongTienVon, ISNULL(hd.TongDoanhThu,0) - ISNULL(tv.TienVon,0) AS LoiNhuan, hd.NgayGiaoDichGanNhat, hd.MaDonVi, hd.TenDonVi, 0
    	FROM
    	(
    	SELECT IDXe, BienSo, SoMay, SoKhung,  MaDoiTuong, TenDoiTuong, DienThoai, MaDonVi, TenDonVi, COUNT(DISTINCT IDPhieuTiepNhan) AS SoLanTiepNhan, COUNT(IDHoaDon) AS SoLuongHoaDon, SUM(DoanhThu) AS TongDoanhThu,
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
    
    	SELECT @SSoLanTiepNhan = SUM(SoLanTiepNhan), @SSoLuongHoaDon = SUM(SoLuongHoaDon), @STongDoanhThu = SUM(TongDoanhThu), @STienVon = SUM(TongTienVon), @SLoiNhuan = SUM(LoiNhuan), @SChiPhi = SUM(ChiPhi) FROM @tblBaoCaoDoanhThu
    
    	SELECT *, CAST(@SSoLanTiepNhan AS FLOAT) AS SSoLanTiepNhan, @SSoLuongHoaDon AS SSoLuongHoaDon, @STongDoanhThu AS STongDoanhThu, @STienVon AS STienVon, @SLoiNhuan AS SLoiNhuan, @SChiPhi AS SChiPhi FROM @tblBaoCaoDoanhThu
    	ORDER BY BienSo
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
    	TongGiamGia FLOAT, TongThanhToan FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
    
    	INSERT INTO @tblHoaDonSuaChua
    	SELECT ptn.ID, ptn.MaPhieuTiepNhan, ptn.NgayVaoXuong, dmx.BienSo, dt.MaDoiTuong, dt.TenDoiTuong, nv.TenNhanVien, hd.ID,
    	hd.MaHoaDon, hd.NgayLapHoaDon, SUM(hdct.SoLuong* hdct.DonGia), SUM(ISNULL(hdct.TienChietKhau, 0)*hdct.SoLuong), hd.TongTienThue, hd.TongChiPhi,
    	hd.TongGiamGia, hd.TongThanhToan - hd.TongTienThue, hd.DienGiai, dv.MaDonVi, dv.TenDonVi FROM Gara_PhieuTiepNhan ptn
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
    	hd.TongGiamGia, hd.TongThanhToan, hd.DienGiai, dv.MaDonVi, dv.TenDonVi;
    
    	DECLARE @tblBaoCaoDoanhThu TABLE(MaPhieuTiepNhan NVARCHAR(MAX), NgayVaoXuong DATETIME, BienSo NVARCHAR(MAX), 
    	MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), CoVanDichVu NVARCHAR(MAX),
    	ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), NgayLapHoaDon DATETIME, TongTienHang FLOAT, TongChietKhau FLOAT, TongTienThue FLOAT, TongChiPhi FLOAT,
    	TongGiamGia FLOAT, TongThanhToan FLOAT, GhiChu NVARCHAR(MAX), MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), GiaVon FLOAT, TienVon FLOAT, LoiNhuan FLOAT)
    
    	INSERT INTO @tblBaoCaoDoanhThu
    	SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, 
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon, hdsc.TongTienHang, hdsc.TongChietKhau, hdsc.TongTienThue, hdsc.TongChiPhi,
    	hdsc.TongGiamGia, hdsc.TongThanhToan, hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, SUM(ISNULL(hdsc.GiaVon,0)) AS GiaVon, SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS TienVon,
    	hdsc.TongThanhToan - SUM(ISNULL(hdsc.GiaVon,0)*ISNULL(hdsc.SoLuongxk,0)) AS LoiNhuan
    	FROM (
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo, 
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	hdsc.ID, hdsc.MaHoaDon, hdsc.NgayLapHoaDon, hdsc.TongTienHang, hdsc.TongChietKhau, hdsc.TongTienThue, hdsc.TongChiPhi,
    	hdsc.TongGiamGia, hdsc.TongThanhToan, hdsc.GhiChu, hdsc.MaDonVi, hdsc.TenDonVi, ISNULL(xkct.GiaVon,0) AS GiaVon, ISNULL(xkct.SoLuong,0) AS SoLuongxk
    	FROM @tblHoaDonSuaChua hdsc
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
		UNION ALL
		SELECT hdsc.MaPhieuTiepNhan, hdsc.NgayVaoXuong, hdsc.BienSo,
    	hdsc.MaDoiTuong, hdsc.TenDoiTuong, hdsc.CoVanDichVu,
    	NULL, '', null, 0, 0, 0, 0, 0, 0, 
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
    	hdsc.TongChietKhau, hdsc.TongChiPhi, hdsc.TongGiamGia, hdsc.TongThanhToan,
    	hdsc.TongTienHang, hdsc.TongTienThue, hdsc.MaDonVi, hdsc.TenDonVi
		
		DECLARE @tblChiPhi TABLE(IDHoaDon UNIQUEIDENTIFIER, TongChiPhi FLOAT);
		INSERT INTO @tblChiPhi
		SELECT hdcp.ID_HoaDon, SUM(ThanhTien) FROM BH_HoaDon_ChiPhi hdcp
		INNER JOIN @tblBaoCaoDoanhThu bcdt ON hdcp.ID_HoaDon = bcdt.ID
		GROUP BY hdcp.ID_HoaDon;

		UPDATE bcdt
		SET bcdt.TongChiPhi = hdcp.TongChiPhi, bcdt.LoiNhuan = bcdt.LoiNhuan - hdcp.TongChiPhi FROM @tblBaoCaoDoanhThu bcdt
		INNER JOIN @tblChiPhi hdcp ON bcdt.ID = hdcp.IDHoaDon;

    	DECLARE @STongTienHang FLOAT,  @SChietKhau FLOAT, @SThue FLOAT, @SChiPhi FLOAT, @SGiamGia FLOAT, @SDoanhThu FLOAT, @STongTienVon FLOAT, @SLoiNhuan FLOAT
    	SELECT @STongTienHang = SUM(TongTienHang), @SChietKhau = SUM(TongChietKhau), @SThue = SUM(TongTienThue),
    	@SChiPhi = SUM(TongChiPhi), @SGiamGia = SUM(TongGiamGia), @SDoanhThu = SUM(TongThanhToan), @STongTienVon = SUM(TienVon), @SLoiNhuan = SUM(LoiNhuan) 
    	FROM @tblBaoCaoDoanhThu
    
    	SELECT MaPhieuTiepNhan, NgayVaoXuong, BienSo, MaDoiTuong, TenDoiTuong, CoVanDichVu , ID AS IDHoaDon, MaHoaDon,
    	NgayLapHoaDon, ISNULL(TongTienHang, 0) AS TongTienHang, ISNULL(TongChietKhau, 0) AS TongChietKhau, ISNULL(TongTienThue, 0) AS TongTienThue, 
    	ISNULL(TongChiPhi, 0) AS TongChiPhi, ISNULL(TongGiamGia, 0) AS TongGiamGia, 
    	ISNULL(TongThanhToan, 0) AS DoanhThu, ISNULL(Tienvon, 0) AS TienVon, ISNULL(LoiNhuan, 0) AS LoiNhuan, GhiChu, MaDonVi, TenDonVi, ISNULL(@STongTienHang, 0) AS STongTienHang, ISNULL(@SChietKhau,0) AS SChietKhau,
    	ISNULL(@SThue,0) AS SThue, ISNULL(@SChiPhi,0) AS SChiPhi, ISNULL(@SGiamGia,0) AS SGiamGia, ISNULL(@SDoanhThu, 0) AS SDoanhThu, ISNULL(@STongTienVon,0) AS STongTienVon,
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
    --	tinh ton dau ky
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
    			left join DM_DoiTuong dt on qhdct.ID_DoiTuong = dt.ID
    			left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
    			left join Quy_KhoanThuChi ktc on qhdct.ID_KhoanThuChi = ktc.ID
    			left join DM_TaiKhoanNganHang tknh on qhdct.ID_TaiKhoanNganHang = tknh.ID
    			left join DM_NganHang nh on qhdct.ID_NganHang = nh.ID
    			where DATEPART(YEAR, qhd.NgayLapHoaDon) = @year
    			and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
    			and (IIF(qhdct.ID_NhanVien is not null, 4, IIF(dt.loaidoituong IS NULL, 1, dt.LoaiDoiTuong)) in (select * from splitstring(@loaiKH)))
    			and qhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    			and (qhdct.DiemThanhToan = 0 or qhdct.DiemThanhToan is null)
    				and qhd.LoaiHoaDon = 11
    			and (qhd.HachToanKinhDoanh = @HachToanKD OR @HachToanKD IS NULL)
    				AND qhdct.HinhThucThanhToan != 6
    				and (dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) OR @ID_NhomDoiTuong = '')
    				and (qhd.PhieuDieuChinhCongNo is null or qhd.PhieuDieuChinhCongNo !='1') 
    			Group by qhd.ID, qhd.LoaiHoaDon, hd.LoaiHoaDon, dt.MaDoiTuong,dt.LoaiDoiTuong, dt.TenDoiTuong_ChuCaiDau, dt.TenDoiTuong_KhongDau, qhdct.ID_KhoanThuChi, 
    			qhd.HachToanKinhDoanh, dt.DienThoai, qhd.MaHoaDon, qhd.NguoiNopTien, hd.MaHoaDon, dt.IDNhomDoiTuongs, qhdct.ID
    		)a
    		where a.LoaiThuChi in (select * from splitstring(@lstThuChi))
    		) b
    				where LoaiTien like @LoaiTien OR @LoaiTien = ''
    			Group by b.ID_KhoanThuChi, b.ThangLapHoaDon, b.ID_DoiTuong, b.ID_HoaDon, b.LoaiThuChi
    		) as c
    			left join Quy_KhoanThuChi ktc on c.ID_KhoanThuChi = ktc.ID
    			Group by c.ID_KhoanThuChi, c.ThangLapHoaDon, ktc.NoiDungThuChi, c.LoaiThuChi
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
    			Case When dtn.ID_NhomDoiTuong is null then
    			Case When dt.LoaiDoiTuong = 1 then '00000010-0000-0000-0000-000000000010' else '30000000-0000-0000-0000-000000000003' end else dtn.ID_NhomDoiTuong end as ID_NhomDoiTuong,
    			Case when qhd.NguoiNopTien is null or qhd.NguoiNopTien = '' then N'Khách lẻ' else qhd.NguoiNopTien end as TenNguoiNop,
    			SUM(qhdct.TienMat) as TienMat,
    			SUM(qhdct.TienGui) as TienGui,
    			SUM(qhdct.TienThu) as TienThu,
    				MAX(DATEPART(MONTH, qhd.NgayLapHoaDon)) as ThangLapHoaDon
    			From Quy_HoaDon qhd 
    			inner join Quy_HoaDon_ChiTiet qhdct on qhd.ID = qhdct.ID_HoaDon
    			left join BH_HoaDon hd on qhdct.ID_HoaDonLienQuan = hd.ID
    			left join DM_DoiTuong dt on qhdct.ID_DoiTuong = dt.ID
    			left join DM_DoiTuong_Nhom dtn on dt.ID = dtn.ID_DoiTuong
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
    				and (dtn.ID_NhomDoiTuong in (select * from splitstring(@ID_NhomDoiTuong)) OR @ID_NhomDoiTuong = '')
    				and (qhd.PhieuDieuChinhCongNo is null or qhd.PhieuDieuChinhCongNo !='1') 
    			Group by qhd.ID,qhd.LoaiHoaDon, hd.LoaiHoaDon, dt.MaDoiTuong,dt.LoaiDoiTuong, dt.TenDoiTuong_ChuCaiDau, dt.TenDoiTuong_KhongDau, qhdct.ID_KhoanThuChi,
    			 qhd.HachToanKinhDoanh, dt.DienThoai, qhd.MaHoaDon, qhd.NguoiNopTien, hd.MaHoaDon, dtn.ID_NhomDoiTuong,qhdct.ID
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
										case when datepart(year,DenNgay) != datepart(year, @dtNow) 
											then datepart(year, @dtNow) else iif(TuNgay < @FromDate, datepart(year, @FromDate), datepart(year,TuNgay)) end end as Nam, 
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
    			Case When gv.ID is null then 0 else  gv.GiaVon end as GiaVon,
    			qd.GiaBan,
    			qd.GiaNhap,
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

			Sql(@"ALTER PROCEDURE [dbo].[GetList_GoiDichVu_afterUseAndTra]
   @IDChiNhanhs nvarchar(max) = null,
   @IDNhanViens nvarchar(max) = null,
   @DateFrom datetime = null,
   @DateTo datetime = null,
   @TextSearch nvarchar(max) = null,
   @CurrentPage int =null,
   @PageSize int = null
AS
BEGIN

	set nocount on;

	if isnull(@CurrentPage,'') =''
		set @CurrentPage = 0
	if isnull(@PageSize,'') =''
		set @PageSize = 10


	DECLARE @tblChiNhanh table (ID nvarchar(max))
	if isnull(@IDChiNhanhs,'') !=''
		insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanhs)		
	else
		set @IDChiNhanhs =''

	DECLARE @tblNhanVien table (ID uniqueidentifier)
	if isnull(@IDNhanViens,'') !=''
		insert into @tblNhanVien select name from dbo.splitstring(@IDNhanViens)		
	else
		set @IDNhanViens=''


	DECLARE @tblSearch TABLE (Name [nvarchar](max))
	DECLARE @count int
	INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!=''
	select @count =  (Select count(*) from @tblSearch)

	DECLARE @VDateFrom DATETIME;
	IF(ISNULL(@DateFrom, '') != '')
	BEGIN
		SET @VDateFrom = @DateFrom;
	END
	ELSE
	BEGIN
		SET @VDateFrom = '1999-01-01';
	END
	DECLARE @VDateTo DATETIME;
	IF(ISNULL(@DateTo, '') != '')
	BEGIN
		SET @VDateTo = @DateTo;
	END
	ELSE
	BEGIN
		SET @VDateTo = DATEADD(DAY, 10, GETDATE());
	END

	DECLARE @tblHoaDonGoiDichVu TABLE(ID UNIQUEIDENTIFIER, MaHoaDon NVARCHAR(MAX), ID_HoaDon UNIQUEIDENTIFIER, ID_BangGia UNIQUEIDENTIFIER,
	ID_NhanVien UNIQUEIDENTIFIER, ID_DonVi UNIQUEIDENTIFIER, NguoiTao NVARCHAR(MAX), ID_Xe UNIQUEIDENTIFIER, DienGiai NVARCHAR(MAX), NgayLapHoaDon DATETIME, 
	TenNhanVien NVARCHAR(MAX), TenDonVi NVARCHAR(MAX),
	ID_DoiTuong UNIQUEIDENTIFIER, TenDoiTuong NVARCHAR(MAX), MaDoiTuong NVARCHAR(MAX), BienSo NVARCHAR(MAX), 
	TongThanhToan FLOAT, PhaiThanhToan FLOAT, TongTienHang FLOAT, TongGiamGia FLOAT, DiemGiaoDich FLOAT,
	LoaiHoaDon INT, TongTienThue FLOAT, TongThueKhachHang FLOAT)

	INSERT INTO @tblHoaDonGoiDichVu
	SELECT 
		hd.ID, 
		hd.MaHoaDon,
		hd.ID_HoaDon,
		hd.ID_BangGia,
		hd.ID_NhanVien,
		hd.ID_DonVi,
		hd.NguoiTao,
		hd.ID_Xe,
		hd.DienGiai,
		hd.NgayLapHoaDon,
		ISNULL(nv.TenNhanVien,'' ) as TenNhanVien,
		dv.TenDonVi,
		hd.ID_DoiTuong,
		ISNULL(dt.TenDoiTuong,N'Khách lẻ' ) as TenDoiTuong,
		ISNULL(dt.MaDoiTuong,'kl' ) as MaDoiTuong,
		xe.BienSo,
		ISNULL(hd.TongThanhToan, hd.PhaiThanhToan) as TongThanhToan,
		ISNULL(hd.PhaiThanhToan, 0) as PhaiThanhToan,
		ISNULL(hd.TongTienHang, 0) as TongTienHang,
		ISNULL(hd.TongGiamGia, 0) as TongGiamGia,
		ISNULL(hd.DiemGiaoDich, 0) as DiemGiaoDich,
		hd.LoaiHoaDon,
		ISNULL(hd.TongTienThue, 0) as TongTienThue ,
		ISNULL(hd.TongThueKhachHang, 0) as TongThueKhachHang
	FROM BH_HoaDon hd
	left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID
	left join DM_DonVi dv on hd.ID_DonVi= dv.ID	
	LEFT JOIN Gara_DanhMucXe xe ON xe.ID = hd.ID_Xe
	LEFT JOIN DM_DoiTuong dt ON hd.ID_DoiTuong = dt.ID	
	WHERE hd.LoaiHoaDon = 19 and hd.ChoThanhToan = 0 
	AND hd.NgayLapHoaDon BETWEEN @VDateFrom AND @VDateTo
	and (@IDNhanViens ='' or exists (select id from @tblNhanVien nv2 where nv.ID= nv2.ID))
	and (@IDChiNhanhs ='' or exists (select id from @tblChiNhanh cn2 where dv.ID= cn2.ID))
	and 
		((select count(Name) from @tblSearch b where     			
		hd.MaHoaDon like '%'+b.Name+'%'
		or hd.NguoiTao like '%'+b.Name+'%'
		or xe.BienSo like '%'+b.Name+'%'	
		or dt.MaDoiTuong like '%'+b.Name+'%'		
		or dt.TenDoiTuong like '%'+b.Name+'%'
		or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
		or dt.DienThoai like '%'+b.Name+'%'		
	)=@count or @count=0)

	-- Hóa đơn mua
	DECLARE @tblTonGoiDichVuChiTiet TABLE(IDGoiDV UNIQUEIDENTIFIER, IDDonViQuiDoi UNIQUEIDENTIFIER, IDChiTietGoiDichVu UNIQUEIDENTIFIER, SoLuongTon FLOAT);
	INSERT INTO @tblTonGoiDichVuChiTiet
	select 
		hd.ID as ID_GoiDV,
		hdct.ID_DonViQuiDoi,
		hdct.ID,
		hdct.SoLuong
	from @tblHoaDonGoiDichVu hd
	inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
	where hdct.ID_ChiTietDinhLuong = hdct.ID  OR hdct.ID_ChiTietDinhLuong is null;

	UPDATE tdv
	SET tdv.SoLuongTon = tdv.SoLuongTon - hdsd.SoLuongSuDung
	FROM @tblTonGoiDichVuChiTiet tdv
	INNER JOIN
	(select ct2.ID_ChiTietGoiDV, SUM(ct2.SoLuong) AS SoLuongSuDung	
	from BH_HoaDon_ChiTiet ct2 
	join BH_HoaDon hd2 on ct2.ID_HoaDon =hd2.ID
	where hd2.ChoThanhToan= 0 and hd2.LoaiHoaDon = 1  and (ct2.ID_ChiTietDinhLuong is null or ct2.ID_ChiTietDinhLuong =ct2.ID)
	GROUP BY ct2.ID_ChiTietGoiDV
	) hdsd
	ON hdsd.ID_ChiTietGoiDV = tdv.IDChiTietGoiDichVu;

	DECLARE @tblTonGoiDichVu TABLE(IDGoiDV UNIQUEIDENTIFIER, SoLuongTon FLOAT);
	INSERT INTO @tblTonGoiDichVu
	SELECT IDGoiDV, SUM(SoLuongTon) FROM @tblTonGoiDichVuChiTiet
	GROUP BY IDGoiDV;
	-- Hóa đơn trả
	UPDATE tdv
	SET tdv.SoLuongTon = tdv.SoLuongTon - hdt.SoLuongTra
	FROM @tblTonGoiDichVu tdv
	INNER JOIN
	(select 
		hd3.ID_HoaDon as ID_HoaDonGoc, 
		SUM(ISNULL(ct3.SoLuong, 0)) AS SoLuongTra
	from BH_HoaDon_ChiTiet ct3
	INNER join BH_HoaDon hd3 on ct3.ID_HoaDon = hd3.ID 
	where hd3.ChoThanhToan =0 AND hd3.ID_HoaDon IS NOT NULL
	group by hd3.ID_HoaDon) hdt ON hdt.ID_HoaDonGoc = tdv.IDGoiDV

	select tblView.*
	into #tblX
	from
	(
		SELECT R.ID, R.MaHoaDon, R.ID_HoaDon, R.ID_BangGia, R.ID_NhanVien, R.ID_DonVi, R.NguoiTao, R.ID_Xe,
		R.DienGiai, R.NgayLapHoaDon, R.TenNhanVien, R.ID_DoiTuong, R.TenDoiTuong, R.MaDoiTuong,
		R.BienSo, R.TongThanhToan, R.PhaiThanhToan, R.TongTienHang, R.TongGiamGia, R.DiemGiaoDich, R.LoaiHoaDon, R.TongTienThue, R.TongThueKhachHang,
		R.TotalRow, R.SoLuongTon AS SoLuongConLai FROM
		(SELECT row_number() over (order by gdv.NgayLapHoaDon desc) as Rn,
					COUNT(gdv.ID) OVER () as TotalRow, * FROM @tblHoaDonGoiDichVu gdv
		INNER JOIN @tblTonGoiDichVu tdv ON gdv.ID = tdv.IDGoiDV
		WHERE tdv.SoLuongTon > 0) R
		WHERE R.Rn BETWEEN (@CurrentPage * @PageSize) + 1 AND @PageSize * (@CurrentPage + 1)
	) tblView
	
		declare @tblID TblID 
		insert into @tblID
		select ID from #tblX
			
			
		declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, BuTruTraHang float,  KhachDaTra float)
		insert into @tblCongNo
		exec HD_GetBuTruTraHang @tblID, 6
	
		select tblView.*,
			isnull(cn.BuTruTraHang,0) as BuTruTra,	
			tblView.PhaiThanhToan - isnull(cn.BuTruTraHang,0) - isnull(cn.KhachDaTra,0) as ConNo		
		from #tblX tblView
		left join @tblCongNo cn on tblView.ID = cn.ID
	
END");

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

--declare @ID_ChiNhanh nvarchar(max) ='D93B17EA-89B9-4ECF-B242-D03B8CDE71DE',
--@timeStart datetime='2021-01-01', @timeEnd datetime ='2023-01-01',
--@IDViTris nvarchar(max) ='',
--@IDBangGias nvarchar(max) ='',
--@LaHoaDonSuaChua varchar(20)='1',
--@BaoHiem int = 3,
--@TrangThai nvarchar(max) ='0,1,2,3',
--@maHD [nvarchar](max) ='',
--@PhuongThucThanhToan nvarchar(max) ='0,1,2,3,4,5',
--@ColumnSort nvarchar(max) ='TongTienHang',
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
	

	declare @tblTrangThai table (TrangThaiHD char(1))
	insert into @tblTrangThai
	select Name from dbo.splitstring(@TrangThai);

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

			---- get allHD co trahang (chi lay truoc 6 thang)
			declare @hdCoTraHang  table (ID uniqueidentifier)
			insert into @hdCoTraHang
			select distinct hd.ID_HoaDon			
			from BH_HoaDon hd
			where hd.LoaiHoaDon= 6
			and hd.ID_HoaDon is not null
			and hd.ChoThanhToan ='0'
			and hd.NgayLapHoaDon > DATEADD(MONTH,-6, @timeStart)

	
	
			declare @tblID TblID
			insert into @tblID
			select ID from @hdCoTraHang
		
			------ get congno of hd coTraHang---
			declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, TongTienHDTra float, KhachDaTra float)
			insert into @tblCongNo
			exec HD_GetBuTruTraHang @tblID, 6

			delete from @hdCoTraHang

		

			----- get list hd from - to
			select hd.ID, hd.ID_HoaDon, hd.ID_DoiTuong, hd.ID_BaoHiem, hd.NgayLapHoaDon, hd.ChoThanhToan
			into #tmpHoaDon
			from BH_HoaDon hd
			where  hd.NgayLapHoadon between @timeStart and @timeEnd					
			and exists (select loai.Loai from @tblLoaiHoaDon loai where hd.LoaiHoaDon = loai.Loai)
			and exists (select cn.ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID) 
					

				;with data_cte
				as(		
				select c.*,
					cn.MaHoaDonGoc,
					cn.LoaiHoaDonGoc,
					isnull(cn.TongTienHDTra,0) as TongTienHDTra,
					c.PhaiThanhToan - isnull(cn.TongTienHDTra,0) as gtriSauTra,
					iif(c.ChoThanhToan is null,0, c.TongThanhToan - isnull(cn.TongTienHDTra,0)- c.KhachDaTra - c.BaoHiemDaTra) as ConNo
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
					hd.KhuyeMai_GiamGia,
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
					ISNULL(hd.TongThanhToan,0) as TongThanhToan,
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
									
							)  d group by d.ID,d.NgayLapHoaDon,ID_HoaDon		
						) thuDH where isFirst= 1
					) soquy group by soquy.ID_HoaDonLienQuan
			) hdSq
			join BH_HoaDon hd on hdSq.ID_HoaDonLienQuan = hd.ID
			left join @tblCongNo cn  on hd.ID = cn.ID
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
			and ((select count(Name) from @tblSearch b where     			
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
	left join @tblCongNo cn  on c.ID = cn.ID
	WHERE (@BaoHiem= 3 or SuDungBaoHiem = @BaoHiem)
	and exists (select tt.TrangThaiHD from @tblTrangThai tt where c.TrangThaiHD= tt.TrangThaiHD)
	and ( @PhuongThucThanhToan ='' or exists(SELECT Name FROM splitstring(c.PTThanhToan) pt join @tblPhuongThuc pt2 on pt.Name = pt2.PhuongThuc))

	),
	
	count_cte
		as (
			select count(ID) as TotalRow,
				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
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
		)
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
			when @ColumnSort='TongTienBHDuyet' then TongTienBHDuyet end DESC					
			
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY

	
	--	drop table #tmpHoaDon


		
End");

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
		--po.MaHoaDon as MaHoaDonGoc,
		--isnull(po.LoaiHoaDon,0) as LoaiHoaDonGoc,
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
	--	left join BH_HoaDon po on hd.ID_HoaDon= po.ID
    	left join  DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
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
					sum(tblTongChi.TienDatCoc) as TienDatCoc
				from
				(
    				select a.ID_HoaDonLienQuan, 
						sum(TienThu) as TienThu,
						sum(a.TienMat) as TienMat,
						sum(a.TienATM) as TienATM,				
						sum(a.ChuyenKhoan) as ChuyenKhoan,
						sum(a.TienDatCoc) as TienDatCoc
    				from(
    					select qct.ID_HoaDonLienQuan,   
						iif(qct.HinhThucThanhToan =1, qct.TienThu, 0) as TienMat,
						iif(qct.HinhThucThanhToan = 2, qct.TienThu,0) as TienATM,
						iif(qct.HinhThucThanhToan = 3, qct.TienThu,0) as ChuyenKhoan,
						iif(qct.HinhThucThanhToan = 6, qct.TienThu,0) as TienDatCoc,
						iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu) as TienThu
    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					join BH_HoaDon hd on qct.ID_HoaDonLienQuan= hd.ID
    					where exists (select Loai from @tblLoaiHD loaiHD where hd.LoaiHoaDon= loaiHD.Loai)	
    					and (qhd.TrangThai= 1 or qhd.TrangThai is null)
    					and hd.NgayLapHoaDon >= @FromDate and hd.NgayLapHoaDon < @ToDate
    					and  exists (select ID from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
    					) a group by a.ID_HoaDonLienQuan

						Union all
						---- get TongChi from PO: chi get hdXuly first
    					select 
							ID,
							TienThu,
							TienMat, 
							TienATM,
							ChuyenKhoan,
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
    		)=@count or @count=0)	
  


			----- get list ID of top 10
			declare @tblID TblID
			insert into @tblID
			select ID from #tmpNhapHang

		
		
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
					sum(TongTienHDTra) as SumTongTienHDTra  
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

		select ID , ID_DoiTuong, ID_BaoHiem, ID_HoaDon,NgayLapHoaDon
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
					isnull(hd.GiamTruThanhToanBaoHiem,0) as  GiamTruThanhToanBaoHiem,

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
    
			begin try
    			exec UpdateTonLuyKeCTHD_whenUpdate @ID_HoaDon, @ID_DonVi, @NgayLapHoaDon
    			exec UpdateGiaVon_WhenEditCTHD @ID_HoaDon, @ID_DonVi, @NgayLapHoaDon
			end try
			begin catch
			end catch
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
		max(table1.TonKho) as TonKho,
		sum(sum(table1.SoLuong)) over ( order by NgayLapHoaDon ) as LuyKeTonKho,
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

			Sql(@"ALTER PROCEDURE [dbo].[ReportValueCard_Balance]
    @TextSearch [nvarchar](max),
    @ID_ChiNhanhs [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),	
    @Status [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    set nocount on;


	set @DateTo = DATEADD(day, 1, @DateTo)
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	with data_cte
    	as (
    		select 
    				tblView.ID, tblView.MaDoiTuong, tblView.TenDoiTuong, 
    				ISNULL(tblView.DienThoai,'') as DienThoaiKhachHang,
    				ISNULL(tblView.SoDuDauKy,0) as SoDuDauKy,
    				ISNULL(tblView.PhatSinhTang,0) as PhatSinhTang,
    				ISNULL(tblView.PhatSinhGiam,0) as PhatSinhGiam,
    				ISNULL(tblView.SoDuDauKy,0) + ISNULL(tblView.PhatSinhTang,0)- ISNULL(tblView.PhatSinhGiam,0) as SoDuCuoiKy,
    				case when tblView.TrangThai_TheGiaTri is null or tblView.TrangThai_TheGiaTri = 1 then N'Đang hoạt động'
    				else N'Ngừng hoạt động' end as TrangThai_TheGiaTri,
    				TrangThai
    		from 
    		(
    			select 
    				dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, 
    				dt.TrangThai_TheGiaTri,
    				case when dt.TrangThai_TheGiaTri is null or dt.TrangThai_TheGiaTri = 1 then '11'
    				else '12' end as TrangThai, -- used to where TrangThai_TheGiaTri (1: all, 11: dang hoat dong, 2. Ngung hoat dong)
    				dt.DienThoai,
    				tblTemp.SoDuDauKy,
    				tblTemp.PhatSinhTang,
    				tblTemp.PhatSinhGiam
    			from DM_DoiTuong dt
    			left join 
    			( 
    				select 
    					ID_DoiTuong,
    					SUM(ISNULL(TongThuTheGiaTri,0)) as TongThuTheGiaTri,
    					SUM(ISNULL(SuDungThe,0)) as SuDungThe,
    					SUM(ISNULL(HoanTraTheGiatri,0)) as HoanTraTheGiaTri,
    					SUM(ISNULL(TongThuTheGiaTri,0))  - SUM(ISNULL(SuDungThe,0)) + SUM(ISNULL(HoanTraTheGiatri,0)) as SoDuDauKy,
    					SUM(ISNULL(PhatSinh_ThuTuThe,0)) + SUM(ISNULL(PhatSinh_HoanTraTheGiatri,0)) + SUM(ISNULL(PhatSinhTang_DieuChinhThe,0)) as PhatSinhTang,
    					SUM(ISNULL(PhatSinh_SuDungThe,0)) + SUM(ISNULL(PhatSinhGiam_DieuChinhThe,0)) as PhatSinhGiam
    
    				from (
							----- ===== Dau ky =======
    					 ---- thu the gtri trước thời gian tìm kiếm (lấy luôn cả gtrị điều chỉnh)
    							 SELECT hd.ID_DoiTuong,
    								  sum(hd.TongTienHang) as TongThuTheGiaTri,
									  0 as  SuDungThe,
    								  0 as  HoanTraTheGiatri,						 
    								  0 as  PhatSinh_ThuTuThe,
    								  0 as  PhatSinh_SuDungThe,
    								  0 as  PhatSinh_HoanTraTheGiatri,
    								  0 as  PhatSinhTang_DieuChinhThe,
    								  0 as  PhatSinhGiam_DieuChinhThe
    							 from BH_HoaDon hd    							 
    							 where hd.NgayLapHoaDon < @DateFrom 
    							 and hd.ChoThanhToan='0' 
								 and hd.LoaiHoaDon in (22,23)								 
    							 group by hd.ID_DoiTuong
    						 
    
    					 union all
    					 ---- su dung the giatri    						
    						SELECT qct.ID_DoiTuong,
								0 as  TongThuTheGiaTri,
    							sum(qct.TienThu)  as SuDungThe,
								0 as  HoanTraTheGiatri,						
    							0 as  PhatSinh_ThuTuThe,
    							0 as  PhatSinh_SuDungThe,
    							0 as  PhatSinh_HoanTraTheGiatri,
    							0 as  PhatSinhTang_DieuChinhThe,
    							0 as  PhatSinhGiam_DieuChinhThe
    						from Quy_HoaDon_ChiTiet qct
    						left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    						where  qhd.NgayLapHoaDon < @DateFrom 
    						and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    						and qhd.LoaiHoaDon = 11
							and qct.HinhThucThanhToan = 4
    						group by qct.ID_DoiTuong
    						 
    
    				 union all
    					  -- hoan tra tien vao the (tang sodu)   						
    						SELECT qct.ID_DoiTuong,
								0 as  TongThuTheGiaTri,
    							0 as  SuDungThe,
    							sum(qct.TienThu) as HoanTraTheGiatri,
								0 as  PhatSinh_ThuTuThe,
    							0 as  PhatSinh_SuDungThe,
    							0 as  PhatSinh_HoanTraTheGiatri,
    							0 as  PhatSinhTang_DieuChinhThe,
    							0 as  PhatSinhGiam_DieuChinhThe
    						from Quy_HoaDon_ChiTiet qct   								
    						left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    						where  qhd.NgayLapHoaDon < @DateFrom 
    						and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    						and qhd.LoaiHoaDon = 12
							and qct.HinhThucThanhToan = 4
    						group by qct.ID_DoiTuong
    						
						 union all
    					  -- giam do hoantracoc			
    					 SELECT hd.ID_DoiTuong,
    							null TongThuTheGiaTri,
								sum(hd.TongTienHang) as SuDungThe,
    							0 as  HoanTraTheGiatri,						 
    							0 as  PhatSinh_ThuTuThe,
    							0 as  PhatSinh_SuDungThe,
    							0 as  PhatSinh_HoanTraTheGiatri,
    							0 as  PhatSinhTang_DieuChinhThe,
    							0 as  PhatSinhGiam_DieuChinhThe								
    					from BH_HoaDon hd    							 
    					where hd.NgayLapHoaDon < @DateFrom 
    					and hd.ChoThanhToan='0' 
						and hd.LoaiHoaDon = 32
    					group by hd.ID_DoiTuong
    
					-----=========== Trong ky ==============
    					 union all
    					   --- thu the gtri tại thời điểm hiện tại
    						SELECT hd.ID_DoiTuong,
    								  0 as  TongThuTheGiaTri,
									  0 as  SuDungThe,
    								  0 as  HoanTraTheGiatri,						 
    								  sum(hd.TongTienHang) as PhatSinh_ThuTuThe,
    								  0 as  PhatSinh_SuDungThe,
    								  0 as  PhatSinh_HoanTraTheGiatri,
    								  0 as  PhatSinhTang_DieuChinhThe,
    								  0 as  PhatSinhGiam_DieuChinhThe
    							 from BH_HoaDon hd    							 
    							 where hd.NgayLapHoaDon between @DateFrom  and @DateTo
    							 and hd.ChoThanhToan='0' 
								 and hd.LoaiHoaDon in (22,23)
    							 group by hd.ID_DoiTuong
    
    				union all
    					 -- su dung the giatri tại thời điểm hiện tại
    						SELECT qct.ID_DoiTuong,
								0 as  TongThuTheGiaTri,
    							null  as SuDungThe,
								0 as  HoanTraTheGiatri,						
    							0 as  PhatSinh_ThuTuThe,
    							sum(qct.TienThu) as PhatSinh_SuDungThe,
    							0 as  PhatSinh_HoanTraTheGiatri,
    							0 as  PhatSinhTang_DieuChinhThe,
    							0 as  PhatSinhGiam_DieuChinhThe
    						from Quy_HoaDon_ChiTiet qct
    						left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    						where  qhd.NgayLapHoaDon between @DateFrom  and @DateTo
    						and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    						and qhd.LoaiHoaDon = 11
							and qct.HinhThucThanhToan = 4
    						group by qct.ID_DoiTuong
    
							---- tang/giam do dieu chinh the hoac hoantra tiencoc
							 union all
							 SELECT hd.ID_DoiTuong,
    								  0 as  TongThuTheGiaTri,
									  0 as  SuDungThe,
    								  0 as  HoanTraTheGiatri,						 
    								  0 as  PhatSinh_ThuTuThe,
    								  0 as  PhatSinh_SuDungThe,
    								  0 as  PhatSinh_HoanTraTheGiatri,
    								  sum(iif(hd.LoaiHoaDon = 32,0, iif(hd.TongTienHang > 0,hd.TongTienHang,0)))  as PhatSinhTang_DieuChinhThe,
    								  sum(iif(hd.LoaiHoaDon = 32, hd.TongTienHang, iif(hd.TongTienHang < 0,-hd.TongTienHang,0)))  as PhatSinhGiam_DieuChinhThe
    							 from BH_HoaDon hd    							 
    							 where hd.NgayLapHoaDon between @DateFrom  and @DateTo
    							 and hd.ChoThanhToan='0' 
								 and hd.LoaiHoaDon in (23,32)
    							 group by hd.ID_DoiTuong   
    
    					union all
    					  -- hoan tra tien the giatri tại thời điểm hiện tại					
    						SELECT qct.ID_DoiTuong,
								0 as  TongThuTheGiaTri,
    							0 as  SuDungThe,
    							0 as  HoanTraTheGiatri,
								0 as  PhatSinh_ThuTuThe,
    							0 as  PhatSinh_SuDungThe,
    							sum(qct.TienThu) as PhatSinh_HoanTraTheGiatri,
    							0 as  PhatSinhTang_DieuChinhThe,
    							0 as  PhatSinhGiam_DieuChinhThe
    						from Quy_HoaDon_ChiTiet qct   								
    						left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    						where  qhd.NgayLapHoaDon between @DateFrom  and @DateTo
    						and (qhd.TrangThai ='1' or qhd.TrangThai is null)
    						and qhd.LoaiHoaDon = 12
							and qct.HinhThucThanhToan = 4
    						group by qct.ID_DoiTuong   
    					) tblDoiTuong_The group by tblDoiTuong_The.ID_DoiTuong
						
    			) tblTemp on dt.ID= tblTemp.ID_DoiTuong
    			where dt.LoaiDoiTuong =1
    				and
    					 
    							((select count(Name) from @tblSearchString b where    
								dt.DienThoai like '%'+b.Name+'%'
    							or dt.MaDoiTuong like '%'+b.Name+'%'
    							or dt.TenDoiTuong like '%'+b.Name+'%'
    							or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    							or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'				
    							)=@count or @count=0)	
    
    		) tblView 
    		where tblView.TrangThai like @Status
    		and ISNULL(tblView.SoDuDauKy,0) + ISNULL(tblView.PhatSinhTang,0)- ISNULL(tblView.PhatSinhGiam,0) > 0
    	),
    	count_cte
    	as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(SoDuDauKy) as TongSoDuDauKy,
    				sum(PhatSinhTang) as TongPhatSinhTang,
    				sum(PhatSinhGiam) as TongPhatSinhGiam,
    				sum(SoDuCuoiKy) as TongSoDuCuoiKy
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaDoiTuong
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
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
