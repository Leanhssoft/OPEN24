namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20240523 : DbMigration
    {
        public override void Up()
        {
            Sql(@"DISABLE TRIGGER dbo.UpdateNgayGiaoDichGanNhat_DMDoiTuong ON dbo.BH_HoaDon");

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
		iif(tblMaxGD.NgayGiaoDichGanNhat is null,'', FORMAT(tblMaxGD.NgayGiaoDichGanNhat,'yyyyMMdd HH:mm')) as NgayGiaoDichF,
		hd1.SoLanDen,
		tblMaxGD.NgayGiaoDichGanNhat,
		GiaTriMua, 
		GiaTriTra,
		DoanhThu
	from DM_DoiTuong dt  
	left join (
		select hd.ID_DoiTuong,
			max(hd.NgayLapHoaDon) as NgayGiaoDichGanNhat
		from BH_HoaDon hd
		where hd.ChoThanhToan is not null
		group by hd.ID_DoiTuong
	)tblMaxGD on dt.id = tblMaxGD.ID_DoiTuong
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
	and dt.TheoDoi like @TrangThaiKhach
	and tblMaxGD.NgayGiaoDichGanNhat >= @NgayGiaoDichFrom and tblMaxGD.NgayGiaoDichGanNhat < @NgayGiaoDichTo
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
				isnull(tblMaxGD.NgayGiaoDichGanNhat,null) as NgayGiaoDichGanNhat,
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
						hd.ID_DoiTuong,
						max(hd.NgayLapHoaDon) as NgayGiaoDichGanNhat
					from BH_HoaDon hd
					where hd.ChoThanhToan =0				
					group by hd.ID_DoiTuong
				)tblMaxGD on dt.ID = tblMaxGD.ID_DoiTuong
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
			a.GhiChu,
			a.TenNhomHangHoa
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
			isnull(nhomhh.TenNhomHangHoa, N'Nhóm mặc định') as TenNhomHangHoa,
			Case when hd.TongTienHang = 0 then 0 else hdct.ThanhTien * (hd.TongGiamGia / hd.TongTienHang) end as GiamGiaHD
    		FROM BH_HoaDon hd
    		inner join BH_HoaDon_ChiTiet hdct on hd.ID = hdct.ID_HoaDon
    		inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
			left join DM_NhomHangHoa nhomhh on hh.ID_NhomHang = nhomhh.ID
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
											then iif(DenNgay < @ToDate, datepart(year, DenNgay), datepart(year,@ToDate))
											else iif(TuNgay < @FromDate, datepart(year, @FromDate), datepart(year,TuNgay)) end 
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

			tn.NguoiLienHeBH as LienHeBaoHiem,
			tn.SoDienThoaiLienHeBH as SoDienThoaiLienHeBaoHiem,

			---- get 2 trường này chỉ mục đích KhuyenMai thooi daay !!!---
			cus.NgaySinh_NgayTLap,
			case when cus.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' 
					else  ISNULL(cus.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as IDNhomDoiTuongs,
			
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
				cpVC.ID_NhaCungCap,
				iif(cpVC.ID_NhaCungCap = tblLuyKe.ID_DoiTuong,KhachDaTra1, KhachDaTra1 + isnull(cpVC.DaChi_BenVCKhac,0)) as KhachDaTra,
				iif(cpVC.ID_NhaCungCap = tblLuyKe.ID_DoiTuong,DaThanhToan1, DaThanhToan1 + isnull(cpVC.DaChi_BenVCKhac,0)) as DaThanhToan,
				iif(cpVC.ID_NhaCungCap = tblLuyKe.ID_DoiTuong,KhachNo1, KhachNo1 - isnull(cpVC.DaChi_BenVCKhac,0)) as KhachNo,

				iif(cpVC.ID_NhaCungCap = tblLuyKe.ID_DoiTuong,0,isnull(cpVC.DaChi_BenVCKhac,0)) as DaChi_BenVCKhac,
					---- nếu hdGoc: get tổng giá trị trả của chính nó ----
					isnull(iif(tblLuyKe.LoaiHoaDonGoc = 3 or tblLuyKe.ID_HoaDon is null, tblLuyKe.TongGiaTriTra,					
						(select dbo.BuTruTraHang_HDDoi(tblLuyKe.ID_HoaDon,tblLuyKe.NgayLapHoaDon,ID_HoaDonGoc, LoaiHoaDonGoc))				
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

					isnull(sq.KhachDaTra,0) as KhachDaTra1,
					isnull(sq.BaoHiemDaTra,0) as BaoHiemDaTra,
					isnull(sq.ThuDatHang,0) as ThuDatHang,

					ISNULL(hd.PhaiThanhToan,0) - ISNULL(sq.KhachDaTra,0) as KhachNo1,
					isnull(sq.Khach_TienMat,0) + ISNULL(sq.BH_TienMat,0) as TienMat,
    				isnull(sq.Khach_TienPOS,0) + ISNULL(sq.BH_TienPOS,0) as TienATM,
    				isnull(sq.Khach_TienCK,0) + ISNULL(sq.BH_TienCK,0) as ChuyenKhoan,
    				isnull(sq.Khach_TienDiem,0) as TienDoiDiem,
    				isnull(sq.Khach_TheGiaTri,0) as ThuTuThe,		
					isnull(sq.Khach_TienCoc,0) + isnull(sq.BH_TienCoc,0) as TienDatCoc,
					isnull(sq.KhachDaTra,0) + isnull(sq.BaoHiemDaTra,0) as DaThanhToan1
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
			) cpVC on tblLuyKe.ID = cpVC.ID_HoaDon
		
		)tblDebit		
		left join Gara_PhieuTiepNhan tn on tblDebit.ID_PhieuTiepNhan = tn.ID
		left join Gara_DanhMucXe xe on tblDebit.ID_Xe = xe.ID
		left join DM_DoiTuong cus on tblDebit.ID_DoiTuong = cus.ID
		left join DM_DoiTuong ncc on tblDebit.ID_NhaCungCap = ncc.ID
		left join DM_DoiTuong bh on tblDebit.ID_BaoHiem = bh.ID
		left join DM_GiaBan bg on tblDebit.ID_BangGia = bg.ID
		left join NS_NhanVien nv on tblDebit.ID_NhanVien = nv.ID
		left join DM_DonVi dv on tblDebit.ID_DonVi = dv.ID
END
");

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
			begin
				----- nếu cập nhật hd, và ngày lập new = ngaylap old --> get chính xác đến từng giây --
				declare @ngayLapHDOld datetime = (select top 1 NgayLapHoaDon from BH_HoaDon where ID= @IdHoaDonUpdate)
				if format (@ngayLapHDOld,'yyyyMMdd HH:mm') = format(@ToDate,'yyyyMMdd HH:mm')
					set @ToDate = @ngayLapHDOld
			end
		
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

			Sql(@"ALTER PROCEDURE [dbo].[UpdateTonLuyKeCTHD_whenUpdate]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDOld [datetime]
AS
BEGIN
    SET NOCOUNT ON;

	----declare @IDHoaDonInput uniqueidentifier, @IDChiNhanhInput uniqueidentifier, @NgayLapHDOld datetime

	----select top 1 @IDHoaDonInput = ID, @IDChiNhanhInput = ID_DonVi, @NgayLapHDOld = NgayLapHoaDon  
	----from BH_HoaDon where MaHoaDon='XH0000003370'


    
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
			or (hd.ChoThanhToan='0'  
					and ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon > @NgayLapHDMin_SubMiliSecond
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

							
				----- get phieuKK theo hanghoa (mỗi hàng hóa có khoảng kiểm kê # nhau) ----
				declare @tblHangKiemKe table (ID_HoaDon uniqueidentifier, NgayKiemKe datetime, ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier)
				insert into @tblHangKiemKe
				select ID_HoaDon, NgayLapHoaDon , ID_HangHoa, ID_LoHang
				from #temp 
				where LoaiHoaDon = 9 and ChoThanhToan = 0 
				group by ID_HoaDon, NgayLapHoaDon,ID_HangHoa, ID_LoHang


				declare @countKiemKe float = (select count(*) from @tblHangKiemKe)			

		

				------ get cthd has hangKiemKe ---			
				select *
				into #cthdHasKiemKe
				from #temp ct
				where ct.LoaiHoaDon != 9 			
				and exists 
					(select ID_HangHoa from @tblHangKiemKe hKK 
					where ct.ID_HangHoa = hKK.ID_HangHoa 
							and (ct.ID_LoHang = hKK.ID_LoHang or ct.ID_LoHang is null and hKK.ID_LoHang is null))


				declare @tblCTHDAfter table (ID_ChiTietHD uniqueidentifier, ID_HoaDon uniqueidentifier, LoaiHoaDon int, MaHoaDon nvarchar (100), NgayLapHoaDon datetime,
							ID_DonVi uniqueidentifier, ID_Checkin uniqueidentifier, YeuCau nvarchar(max),
							ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier, TonLuyKe float)	

				if @countKiemKe >0
				begin
															
						------ duyet phieuKiemKe  --
						declare @idHoaDon uniqueidentifier, @ngayKiemKe datetime, @idHangHoa uniqueidentifier, @idLoHang  uniqueidentifier
						declare _curKK cursor for
						select ID_HoaDon, NgayKiemKe, ID_HangHoa, ID_LoHang
						from @tblHangKiemKe 
						order by NgayKiemKe
						open _curKK
						FETCH NEXT FROM _curKK
						INTO @idHoaDon, @ngayKiemKe,@idHangHoa,@idLoHang
						WHILE @@FETCH_STATUS = 0
						BEGIN   
						
								-------- get ctKK (with idhanghoa, idlohang) trong khoang thoi gian ---
								declare @ngayKiemKeNext datetime 
									= (select top 1 NgayKiemKe from @tblHangKiemKe 
										where NgayKiemKe > @ngayKiemKe and ID_HangHoa = @idHangHoa
										and (ID_LoHang = @idLoHang or ID_LoHang is null and @idLoHang is null)
										order by NgayKiemKe
										)

							
								----- tinh TonLuyKe theo giai doan kiem ke ---
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
								join
								(
									------ get tondauky from phieuKiemKe ----
									select 
										ctAll.ID_HangHoa,
										ctAll.ID_LoHang,
										ctAll.TonLuyKe
									from #temp ctAll 
									where ctAll.ID_HoaDon = @idHoaDon
									and ID_HangHoa = @idHangHoa and (ID_LoHang = @idLoHang or ID_LoHang is null and @idLoHang is null)
								)ctKK on ct.ID_HangHoa = ctKK.ID_HangHoa
								and (ct.ID_LoHang = ctKK.ID_LoHang or ct.ID_LoHang is null and ctKK.ID_LoHang is null)
								where ct.NgayLapHoaDon >= @ngayKiemKe	
								and (@ngayKiemKeNext is null or ct.NgayLapHoaDon < @ngayKiemKeNext)						
		
						FETCH NEXT FROM _curKK
						INTO @idHoaDon, @ngayKiemKe, @idHangHoa,@idLoHang

						END
						CLOSE _curKK;
						DEALLOCATE _curKK;					
				end

				----- hàng thuộc phiếu kk, nhưng có ngày lập < ngày kiểm kê --> tính theo tondauky  ----
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
				from #cthdHasKiemKe ct 
				left join #tblTonKhoDK lkdk on ct.ID_HangHoa = lkdk.ID_HangHoa 
					and (ct.ID_LoHang = lkdk.ID_LoHang or ct.ID_LoHang is null and lkdk.ID_LoHang is null)
				where not exists (select id from @tblCTHDAfter ctAfter where ct.ID = ctAfter.ID_ChiTietHD)
		

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

    
    	
    		 ----- neu update NhanHang --> goi ham update TonKho 2 lan
    		 ---- update GiaVon neu tontai phieu NhapHang,ChuyenHang/NhanHang, DieuChinhGiaVon 
    		declare @count2 float = (select count(ID_HoaDon) from #temp where LoaiHoaDon in (4,7,10, 18))
    		select ISNULL(@count2,0) as UpdateGiaVon, ISNULL(@countKiemKe,0) as UpdateKiemKe, @NgayLapHDMin as NgayLapHDMin


			----drop table #temp
			----drop table #tblTonKhoDK
			----drop table #cthdHasKiemKe
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
					isnull(bh.MaSoThue,'') as BH_MaSoThue,
		
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
						---where hd.ChoThanhToan='0'
						

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
				sum(BHThanhToanTruocThue) as SumBHThanhToanTruocThue,
				sum(GiamTruThanhToanBaoHiem) as SumGiamTruThanhToanBaoHiem
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
			when @ColumnSort='VAT' then TongTienThue end DESC,
			case when @SortBy <>'ASC' then 0
			when @ColumnSort='GiamTruThanhToanBaoHiem' then GiamTruThanhToanBaoHiem end ASC,
			case when @SortBy <>'DESC' then 0
			when @ColumnSort='GiamTruThanhToanBaoHiem' then GiamTruThanhToanBaoHiem end DESC
			
		OFFSET (@CurrentPage* @PageSize) ROWS
		FETCH NEXT @PageSize ROWS ONLY

	
		drop table #tmpHoaDon


		
End");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDonDatHang]	
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @maHD [nvarchar](max),
	@ID_NhanVienLogin uniqueidentifier,
	@NguoiTao nvarchar(max),
	@TrangThai nvarchar(max),
	@ColumnSort varchar(max),
	@SortBy varchar(max),
	@CurrentPage int,
	@PageSize int,
	@LaHoaDonSuaChua nvarchar(10)
AS
BEGIN
	set nocount on;

	declare @tblNhanVien table (ID uniqueidentifier)
	insert into @tblNhanVien
	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanh,'DatHang_XemDS_PhongBan','DatHang_XemDS_HeThong');

	declare @tblChiNhanh table (ID varchar(40))
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@ID_ChiNhanh)

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
	

with data_cte
as
(
    SELECT 
    	c.ID,
    	c.MaHoaDon,
    	c.LoaiHoaDon,
    	c.NgayLapHoaDon,
    	c.TenDoiTuong,
    	c.Email,
    	c.DienThoai,
    	c.ID_NhanVien,
    	c.ID_DoiTuong,
    	c.ID_BangGia,
		c.ID_BaoHiem,
    	c.ID_DonVi,
		c.ID_PhieuTiepNhan,
		c.TongThanhToan,
    	c.YeuCau,
		'' as MaHoaDonGoc,
		c.MaSoThue,
		c.TaiKhoanNganHang,
		ISNULL(c.MaDoiTuong,'') as MaDoiTuong,
    	ISNULL(c.NguoiTaoHD,'') as NguoiTaoHD,
    	c.DiaChiKhachHang,
		c.NgaySinh_NgayTLap,
    	c.KhuVuc,
    	c.PhuongXa,
    	c.TenDonVi,
		c.DiaChiChiNhanh,
    	c.DienThoaiChiNhanh,
    	c.TenNhanVien,
    	c.DienGiai,
    	c.TenBangGia,
    	c.TongTienHang, c.TongGiamGia, c.PhaiThanhToan, c.ConNo,
		c.TienMat,
		c.TienATM,
		c.ChuyenKhoan,
		c.KhachDaTra,c.TongChietKhau,c.TongTienThue, c.ThuTuThe, c.TongChiPhi,
    	c.TrangThai,
		c.TrangThaiHD,
		c.TrangThaiText,
    	c.TheoDoi,
    	c.TenPhongBan,
		c.ChoThanhToan,
		c.ChiPhi_GhiChu,
    	'' as HoaDon_HangHoa, -- string contail all MaHangHoa,TenHangHoa of HoaDon
		 c.MaPhieuTiepNhan,
		c.BienSo,
		c.MaBaoHiem, c.TenBaoHiem, c.BH_SDT, c.BH_MaSoThue,
		c.LienHeBaoHiem, c.SoDienThoaiLienHeBaoHiem,
		c.PhaiThanhToanBaoHiem, c.PTThueBaoHiem,
		TongTienBHDuyet, c.PTThueHoaDon, c.TongTienThueBaoHiem, SoVuBaoHiem,KhauTruTheoVu, 
		PTGiamTruBoiThuong, GiamTruBoiThuong, BHThanhToanTruocThue

    	FROM
    	(
    		select 
    		a.ID as ID,
    		bhhd.MaHoaDon,
    		bhhd.LoaiHoaDon,
    		bhhd.ID_NhanVien,
    		bhhd.ID_DoiTuong,
    		bhhd.ID_BangGia,
    		bhhd.NgayLapHoaDon,
    		bhhd.YeuCau,
    		bhhd.ID_DonVi,
			bhhd.ID_BaoHiem,
			
    		CASE 
    			WHEN dt.TheoDoi IS NULL THEN 
    				CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    			ELSE dt.TheoDoi
    		END AS TheoDoi,
			dt.MaDoiTuong,
			dt.MaSoThue,
			dt.TaiKhoanNganHang,
			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
    		ISNULL(dt.TenDoiTuong_KhongDau, N'khach le') as TenDoiTuong_KhongDau,
			ISNULL(dt.TenDoiTuong_ChuCaiDau, N'kl') as TenDoiTuong_ChuCaiDau,
			dt.NgaySinh_NgayTLap,
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
    		ISNULL(vt.TenViTri,'') as TenPhongBan,
			ceiling(isnull(bhhd.TongTienHang,0)) as TongTienHang,
    		ceiling(isnull(bhhd.TongGiamGia,0)) as TongGiamGia,    		
			ceiling(isnull(bhhd.PhaiThanhToan,0)) as PhaiThanhToan,
			CAST(ROUND(bhhd.TongTienThue, 0) as float) as TongTienThue,
			isnull(bhhd.TongChiPhi,	0) as TongChiPhi,
			bhhd.ChiPhi_GhiChu,
    		a.KhachDaTra,
			a.ThuTuThe,
			a.TienMat,
			a.TienATM,
			a.ChuyenKhoan,
    		bhhd.TongChietKhau,
			bhhd.ID_PhieuTiepNhan,					
			bhhd.TongThanhToan,
			bhhd.TongThanhToan - a.KhachDaTra as ConNo,
			bhhd.ChoThanhToan,

			isnull(bhhd.PTThueHoaDon,0) as PTThueHoaDon,			
			isnull(bhhd.PTThueBaoHiem,0) as PTThueBaoHiem,
			isnull(bhhd.TongTienThueBaoHiem,0) as TongTienThueBaoHiem,
			isnull(bhhd.SoVuBaoHiem,0) as SoVuBaoHiem,
			isnull(bhhd.KhauTruTheoVu,0) as KhauTruTheoVu,
			isnull(bhhd.TongTienBHDuyet,0) as TongTienBHDuyet,
			isnull(bhhd.PTGiamTruBoiThuong,0) as PTGiamTruBoiThuong,
			isnull(bhhd.GiamTruBoiThuong,0) as GiamTruBoiThuong,
			isnull(bhhd.BHThanhToanTruocThue,0) as BHThanhToanTruocThue,
			isnull(bhhd.PhaiThanhToanBaoHiem,0) as PhaiThanhToanBaoHiem,

			isnull(bh.TenDoiTuong,'') as TenBaoHiem,
			isnull(bh.MaDoiTuong,'') as MaBaoHiem,
			isnull(bh.DienThoai,'') as BH_SDT,
			isnull(bh.MaSoThue,'') as BH_MaSoThue,
			iif(bhhd.ID_BaoHiem is null,'',tn.NguoiLienHeBH) as LienHeBaoHiem,
			iif(bhhd.ID_BaoHiem is null,'',tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,
			isnull(tn.MaPhieuTiepNhan,'') as MaPhieuTiepNhan,
			isnull(xe.BienSo,'') as BienSo,
			iif(bhhd.ID_PhieuTiepNhan is null, '0','1') as LaHoaDonSuaChua,
			case bhhd.ChoThanhToan
				when 1 then N'Phiếu tạm' 
				when 0 then 
					case bhhd.YeuCau
						when '2' then  N'Đang giao hàng' 
						when '3' then  N'Hoàn thành' 
						else N'Đã lưu' end
				else  N'Đã hủy'
				end as TrangThai,
		 
			case bhhd.ChoThanhToan
				when 1 then N'Chờ duyệt'
				when 0 then 
					case bhhd.YeuCau
						when '2' then  N'Đang xử lý' 
						when '3' then  N'Hoàn thành' 
						else N'Đã duyệt' end
				else  N'Đã hủy'
			end as TrangThaiText,
		
			case bhhd.ChoThanhToan
				when 1 then '1'
				when 0 then 
					case bhhd.YeuCau
						when 2 then  '2'
						when 3 then '3'
						else '0' end
				else '4' end as TrangThaiHD -- used to where
    		FROM
    		(
    			select 
    			b.ID,
				SUM(ISNULL(b.ThuTuThe, 0)) as ThuTuThe,
				SUM(ISNULL(b.TienMat, 0)) as TienMat,
				SUM(ISNULL(b.TienATM, 0)) as TienATM,
    			SUM(ISNULL(b.TienCK, 0)) as ChuyenKhoan,
    			SUM(ISNULL(b.KhachDaTra, 0)) as KhachDaTra

    			from
    			(
					-- get infor PhieuThu from HDDatHang (HuyPhieuThu (qhd.TrangThai ='0')
    				Select 
    					bhhd.ID,
						Case when qhd.TrangThai = 0 then 0 else Case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.ThuTuThe, 0) else -ISNULL(hdct.ThuTuThe, 0) end end as ThuTuThe,
						case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.TienMat, 0) else -ISNULL(hdct.TienMat, 0) end end as TienMat,
						case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = 11 then case when TaiKhoanPOS = 1 then ISNULL(hdct.TienGui, 0) else 0 end else -ISNULL(hdct.TienGui, 0) end end as TienATM,							
						case when qhd.TrangThai = 0 then 0 else case when qhd.LoaiHoaDon = 11 then case when TaiKhoanPOS = 0 then ISNULL(hdct.TienGui, 0) else 0 end else -ISNULL(hdct.TienGui, 0) end end as TienCK,
    					Case when bhhd.ChoThanhToan is null OR qhd.TrangThai='0' then 0 else case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.Tienthu, 0) else -ISNULL(hdct.Tienthu, 0) end end as KhachDaTra					
   				from BH_HoaDon bhhd
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on hdct.ID_HoaDon = qhd.ID 	
					left join DM_TaiKhoanNganHang tk on tk.ID= hdct.ID_TaiKhoanNganHang					
    				where bhhd.LoaiHoaDon = '3'
					and bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd 
					and exists (select ID from @tblChiNhanh cn where bhhd.ID_DonVi = cn.ID)
    
    				union all
					-- get infor PhieuThu/Chi from HDXuLy
    				Select
    					hdt.ID,
						Case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then 0 else Case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.ThuTuThe, 0) else -ISNULL(hdct.ThuTuThe, 0) end end as ThuTuThe,		
						Case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then 0 else Case when qhd.LoaiHoaDon= 11 then ISNULL(hdct.TienMat, 0) else -ISNULL(hdct.TienMat, 0) end end as TienMat,			
						case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then 0 else case when qhd.LoaiHoaDon = 11 then case when TaiKhoanPOS = 1 then ISNULL(hdct.TienGui, 0) else 0 end else -ISNULL(hdct.TienGui, 0) end end as TienATM,
						case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then 0 else case when qhd.LoaiHoaDon = 11 then case when TaiKhoanPOS = 0 then ISNULL(hdct.TienGui, 0) else 0 end else -ISNULL(hdct.TienGui, 0) end end as TienCK,
  						Case when bhhd.ChoThanhToan is null or qhd.TrangThai='0' then (Case when qhd.LoaiHoaDon = 11 or qhd.TrangThai='0' then 0 else -ISNULL(hdct.Tienthu, 0) end)
    						else (Case when qhd.LoaiHoaDon = 11 then ISNULL(hdct.Tienthu, 0) else -ISNULL(hdct.Tienthu, 0) end) end as KhachDaTra
    				from BH_HoaDon bhhd
    				inner join BH_HoaDon hdt on (bhhd.ID_HoaDon = hdt.ID and hdt.ChoThanhToan = '0')
    				left join Quy_HoaDon_ChiTiet hdct on bhhd.ID = hdct.ID_HoaDonLienQuan
    				left join Quy_HoaDon qhd on (hdct.ID_HoaDon = qhd.ID)
					left join DM_TaiKhoanNganHang tk on tk.ID= hdct.ID_TaiKhoanNganHang		
    				where hdt.LoaiHoaDon = '3' 
					and bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd 
					and exists (select ID from @tblChiNhanh cn where bhhd.ID_DonVi = cn.ID)
    			) b
    			group by b.ID 
    		) as a
    		inner join BH_HoaDon bhhd on a.ID = bhhd.ID
    		left join DM_DoiTuong dt on bhhd.ID_DoiTuong = dt.ID
			left join DM_DoiTuong bh on bhhd.ID_BaoHiem = bh.ID
    		left join DM_DonVi dv on bhhd.ID_DonVi = dv.ID
    		left join NS_NhanVien nv on bhhd.ID_NhanVien = nv.ID 
    		left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    		left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    		left join DM_GiaBan gb on bhhd.ID_BangGia = gb.ID
    		left join DM_ViTri vt on bhhd.ID_ViTri = vt.ID    
			left join Gara_PhieuTiepNhan tn on bhhd.ID_PhieuTiepNhan = tn.ID
			left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID	
			where bhhd.LoaiHoaDon = 3 
			and
			bhhd.NgayLapHoadon >= @timeStart and bhhd.NgayLapHoaDon < @timeEnd 
    		) as c
			where exists( select * from @tblNhanVien nv where nv.ID= c.ID_NhanVien or c.NguoiTaoHD= @NguoiTao)
			and exists( select Name from dbo.splitstring(@TrangThai) tt where  c.TrangThaiHD  = tt.Name)
			and exists (select Name from dbo.splitstring(@LaHoaDonSuaChua) tt where c.LaHoaDonSuaChua = tt.Name)
			and
				((select count(Name) from @tblSearch b where     			
				c.MaHoaDon like '%'+b.Name+'%'
				or c.NguoiTaoHD like '%'+b.Name+'%'
				or c.TenNhanVien like '%'+b.Name+'%'
				or c.TenNhanVienKhongDau like '%'+b.Name+'%'
				or c.DienGiai like '%'+b.Name+'%'
				or c.MaDoiTuong like '%'+b.Name+'%'		
				or c.TenDoiTuong like '%'+b.Name+'%'
				or c.TenDoiTuong_KhongDau like '%'+b.Name+'%'
				or c.DienThoai like '%'+b.Name+'%'	
				or c.MaPhieuTiepNhan like '%'+b.Name+'%'
				or c.BienSo like '%'+b.Name+'%'	
				)=@count or @count=0)	
		
    ),
		count_cte
		as (
			select count(ID) as TotalRow,
				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
				sum(TongTienHang) as SumTongTienHang,
				sum(TongGiamGia) as SumTongGiamGia,
					sum(TongChiPhi) as SumTongChiPhi,
				sum(KhachDaTra) as SumKhachDaTra,	
				sum(PhaiThanhToan) as SumPhaiThanhToan,			
				sum(TongThanhToan) as SumTongThanhToan,				
				sum(ThuTuThe) as SumThuTuThe,				
				sum(TienMat) as SumTienMat,
				sum(TienATM) as SumPOS,
				sum(ChuyenKhoan) as SumChuyenKhoan,				
				sum(TongTienThue) as SumTongTienThue,
				sum(ConNo) as SumConNo
			from data_cte
		)
		select dt.*, cte.*		
		from data_cte dt
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
			case when @SortBy <>'DESC' then ''
			when @ColumnSort='MaPhieuTiepNhan' then dt.MaPhieuTiepNhan end DESC,
			case when @SortBy <>'ASC' then ''
			when @ColumnSort='MaPhieuTiepNhan' then dt.MaPhieuTiepNhan end ASC,
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
							----- nếu bên vc là chính NCC nhập hàng ----
							and hd.ID_DoiTuong != cp.ID_NhaCungCap
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

			Sql(@"ALTER PROCEDURE [dbo].[GetCTHDSuaChua_afterXuatKho]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    set nocount on
    
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
			qd.GiaNhap, ----- used to nhaphang from hoadon/baogia
			isnull(tblVTtri.TenViTris,'') as ViTriKho,
    		qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    		qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, hh.TenHangHoa, CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach, hh.ID_NhomHang as ID_NhomHangHoa, ISNULL(hh.GhiChu,'') as GhiChuHH
    	from
    	(
    			select 
    				 ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon,
					 max(ctsc.ChatLieu) as ChatLieu,
    				 max(ctsc.ID_DonVi) as ID_DonVi,
    				 sum(SoLuong) as SoLuongMua,
    				 sum(SoLuongXuat) as SoLuongXuat,
    				 sum(SoLuong) - isnull(sum(SoLuongXuat),0) as SoLuong
    			from
    			(
    			select sum(ct.SoLuong) as SoLuong,
    				0 as SoLuongXuat,
					max(ct.ChatLieu) as ChatLieu,
    				ct.ID_DonViQuiDoi,
    				ct.ID_LoHang,
    				ct.ID_HoaDon,
    				hd.ID_DonVi
    			from BH_HoaDon_ChiTiet ct
    			join BH_HoaDon hd on hd.ID= ct.ID_HoaDon
    			where ct.ID_HoaDon= @ID_HoaDon
				and (ct.ChatLieu is null or ct.ChatLieu !='5')
    			and (ct.ID_ChiTietDinhLuong != ct.ID or ct.ID_ChiTietDinhLuong is null)
				and ct.ID_LichBaoDuong is null ---- khong xuat hang bao duong
    			group by ct.ID_DonViQuiDoi, ct.ID_LoHang,ct.ID_HoaDon,hd.ID_DonVi
    
    			union all
    			-- get cthd daxuat kho
    			select 0 as SoLuong,
    				sum(ct.SoLuong) as SoLuongXuat,
					max(ct.ChatLieu) as ChatLieu,
    				ct.ID_DonViQuiDoi,
    				ct.ID_LoHang,
    				@ID_HoaDon as ID_HoaDon,
    				'00000000-0000-0000-0000-000000000000' as ID_DonVi
    			from BH_HoaDon_ChiTiet ct
    			join BH_HoaDon hd on ct.ID_HoaDon= hd.ID
    			where hd.ID_HoaDon= @ID_HoaDon
    			and hd.ChoThanhToan='0'
				and hd.LoaiHoaDon= 8
    			group by ct.ID_DonViQuiDoi, ct.ID_LoHang
    			)ctsc
    			group by ctsc.ID_DonViQuiDoi, ctsc.ID_LoHang,ctsc.ID_HoaDon
    	) cthd
    	join DonViQuiDoi qd on cthd.ID_DonViQuiDoi= qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
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
    	left join DM_LoHang lo on cthd.ID_LoHang= lo.ID and hh.ID= lo.ID_HangHoa
    	left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
    	left join DM_HangHoa_TonKho tk on (qd.ID = tk.ID_DonViQuyDoi and (lo.ID = tk.ID_LoHang or lo.ID is null) and  tk.ID_DonVi = cthd.ID_DonVi)
    	left join DM_GiaVon gv on (qd.ID = gv.ID_DonViQuiDoi and (lo.ID = gv.ID_LoHang or lo.ID is null) and gv.ID_DonVi = cthd.ID_DonVi) -- lay giavon hientai --> xuatkho gara tu hdsc
    	where hh.LaHangHoa= 1
END");

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
			select 
				hd.ID as ID_HoaDon,
				ct.ID as ID_ChiTietHoaDon,
				hd.NgayLapHoaDon,
				ct.SoLuong,
				qd.ID_HangHoa,
				ct.ID_LoHang,
				ct.ID_DonViQuiDoi,
				0 as TonDauKy,
				qd.TyLeChuyenDoi
			into #ctNeed
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
								from #ctNeed ctNeed 
								where ctNeed.ID_HangHoa = qd.ID_HangHoa 
								and (ctNeed.ID_LoHang = ct.ID_LoHang or (ctNeed.ID_LoHang is null and ct.ID_LoHang is null))
								------ chỉ lấy những hóa đơn có ngày lập < ngày kiểm kê (có thể có nhiều khoảng ngày kiểm kê )---
								AND ((hd.ID_DonVi = @IDChiNhanhInput and hd.NgayLapHoaDon <  ctNeed.NgayLapHoaDon and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    							or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanhInput and  hd.NgaySua < ctNeed.NgayLapHoaDon ))
								)
					)cthdLienQuan
		
			
			

			------ sắp xếp theo thứ tự ngày lập ---> get TonDauKy của mỗi hóa đơn ---
			select 
					ID_ChiTietHoaDon,
					ID_LoHang,
					ID_HangHoa,
					TonLuyKe,
					NgayLapHoaDon,					
					ROW_NUMBER() over (partition by ID_HangHoa, ID_LoHang order by NgayLapHoaDon ) as RN
			into #tblNew
			from
			(
					select 
						ID_ChiTietHoaDon,
							ID_LoHang,
							ID_HangHoa,
							TonLuyKe,
							NgayLapHoaDon
					from #cthdLienQuan

					UNION ALL

					select 
						ID_ChiTietHoaDon,
							ID_LoHang,
							ID_HangHoa,
							TonDauKy,
							NgayLapHoaDon
					from #ctNeed
			)tbl
		
		------- update TonDauKy for each cthd ----
		update t1 set t1.TonLuyKe = isnull(t2.TonLuyKe,0)
		from #tblNew t1
		left join #tblNew t2 on t1.ID_HangHoa = t2.ID_HangHoa
			and (t1.ID_LoHang = t2.ID_LoHang or t1.ID_LoHang is null and t2.ID_LoHang is null)
			and t1.RN = t2.RN + 1
					


			---------- update TonkhoDB, SoLuongLech, GiaTriLech to BH_HoaDon_ChiTiet----
			update ctkiemke
			set	ctkiemke.TienChietKhau = ctNeed.TonDauKy, 
    			ctkiemke.SoLuong = ctkiemke.ThanhTien - ctNeed.TonDauKy, ---- soluonglech
    			ctkiemke.ThanhToan = ctkiemke.GiaVon * (ctkiemke.ThanhTien - ctNeed.TonDauKy) --- gtrilech = soluonglech * giavon
			from BH_HoaDon_ChiTiet ctkiemke
			join (
					------- vì TonLuyKe đang tính theo dvqd ---> TonDauKy cũng lấy theo dvqd --
				select ctNeed.ID_ChiTietHoaDon, 					
					ctNew.TonLuyKe/ ctNeed.TyLeChuyenDoi as TonDauKy
				from #ctNeed ctNeed
				join #tblNew ctNew on ctNeed.ID_ChiTietHoaDon = ctNew.ID_ChiTietHoaDon							
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
			drop table #cthdLienQuan
			drop table #tblNew

END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateGiaVon_WhenEditCTHD]
   @IDHoaDonInput [uniqueidentifier],
   @IDChiNhanh [uniqueidentifier],
   @NgayLapHDMin [datetime] ----  @NgayLapHDMin: đang lấy ngày lập hóa đơn cũ
AS
BEGIN
    SET NOCOUNT ON;	
		----declare @IDHoaDonInput uniqueidentifier ='35367DCE-98B0-437F-A95F-7A0FF9BEAC08',
		----@IDChiNhanh uniqueidentifier ='D93B17EA-89B9-4ECF-B242-D03B8CDE71DE',
		----@NgayLapHDMin  datetime ='2024-03-14 11:28:00.000'


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
			and	((hd.ID_DonVi = @IDChiNhanh and hd.NgayLapHoaDon >= @NgayLapHDMinNew
    				and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null))
    				or (hd.YeuCau = '4'  and hd.ID_CheckIn = @IDChiNhanh and hd.NgaySua >= @NgayLapHDMinNew)))			
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

		--select * from #tblGVDauKy

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


			------ update GiaVonDauKy cho hd co NgayLapMin --: dựa vào giá vốn này để tính GV cho các hóa đơn tiếp theo ----
			update ctNeed 
				set ctNeed.GiaVonDauKy = isnull(gvDK.GiaVonDauKy,0)				
			from @cthd_NeedUpGiaVon ctNeed
			left join #tblGVDauKy gvDK on ctNeed.ID_HangHoa = gvDK.ID_HangHoa 
				and (ctNeed.ID_LoHang = gvDK.ID_LoHang or (ctNeed.ID_LoHang is null and gvDK.ID_LoHang is null)) 
			where ctNeed.RN = 1

		
    		   			
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
    		
			

		
			drop table #tblTonLuyKe
			drop table #tblGVDauKy
			drop table #gvHienTai
			drop table #ctTraHang

    		END --- end beginOut

			------ alway remove cthd if has delete ----
			delete from BH_HoaDon_ChiTiet where ID_HoaDon = @IDHoaDonInput and ChatLieu='5'
    		
END");

        }
        
        public override void Down()
        {
        }
    }
}
