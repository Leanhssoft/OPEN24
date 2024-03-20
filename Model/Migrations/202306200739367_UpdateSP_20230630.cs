namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    //20230620
    public partial class UpdateSP_20230630 : DbMigration
    {
        public override void Up()
        {
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


        }
        
        public override void Down()
        {
        }
    }
}
