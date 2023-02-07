namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20220119 : DbMigration
    {
        public override void Up()
        {
            CreateStoredProcedure(name: "[dbo].[GetHangHoaDatLichChiTiet]", parametersAction: p => new
            {
                Id = p.Guid()
            }, body: @"SET NOCOUNT ON;

    -- Insert statements for procedure here
	SELECT hh.ID, dvqd.MaHangHoa, hh.TenHangHoa, hh.LoaiHangHoa, dvqd.GiaBan AS DonGia FROM CSKH_DatLich_HangHoa dlhh
	INNER JOIN DM_HangHoa hh ON hh.ID = dlhh.IDHangHoa
	INNER JOIN DonViQuiDoi dvqd ON dvqd.ID_HangHoa = hh.ID
	WHERE dvqd.LaDonViChuan = 1
	AND dlhh.IDDatLich = @Id;");

            CreateStoredProcedure(name: "[dbo].[GetListDatLich]", parametersAction: p => new
            {
                IdChiNhanhs = p.String(),
                ThoiGianFrom = p.DateTime(),
                ThoiGianTo = p.DateTime(),
                TrangThais = p.String(20),
                TextSearch = p.String(),
                CurrentPage = p.Int(),
                PageSize = p.Int()
            }, body: @"SET NOCOUNT ON;
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
	dl.SoDienThoai, ISNULL(dt.TenDoiTuong, dl.TenDoiTuong) AS TenKhachHang, ISNULL(dt.DiaChi, dl.DiaChi) AS DiaChi,
	ISNULL(dt.NgaySinh_NgayTLap, dl.NgaySinh) AS NgaySinh, dl.IDXe, dl.BienSo, dl.LoaiXe AS MauXe, dl.TrangThai, dv.TenDonVi AS TenChiNhanh
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
	dl.SoDienThoai, ISNULL(dt.TenDoiTuong, dl.TenDoiTuong) AS TenKhachHang, ISNULL(dt.DiaChi, dl.DiaChi) AS DiaChi,
	ISNULL(dt.NgaySinh_NgayTLap, dl.NgaySinh) AS NgaySinh, dl.IDXe, dl.BienSo, dl.LoaiXe AS MauXe, dl.TrangThai, dv.TenDonVi AS TenChiNhanh
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
			END");

            CreateStoredProcedure(name: "[dbo].[GetListHangHoaDatLichCheckin]",
                body: @"SET NOCOUNT ON;

	-- Insert statements for procedure here
	DECLARE @tblResult TABLE (ID UNIQUEIDENTIFIER, MaHangHoa NVARCHAR(max), TenHangHoa NVARCHAR(MAX), GhiChu NVARCHAR(MAX), DonGia FLOAT, URLAnh NVARCHAR(MAX));
	INSERT INTO @tblResult
	SELECT hh.ID, dvqd.MaHangHoa, hh.TenHangHoa, hh.GhiChu, dvqd.GiaBan, '' FROM DM_HangHoa hh
	INNER JOIN DonViQuiDoi dvqd ON hh.ID = dvqd.ID_HangHoa
	WHERE dvqd.LaDonViChuan = 1 and hh.HienThiDatLich = 1
	ORDER BY TenHangHoa;

	UPDATE trs
	SET trs.URLAnh = a.URLAnh
	FROM @tblResult trs
	LEFT JOIN
	(SELECT hha.ID_HangHoa, MAX(hha.URLAnh) AS URLAnh FROM DM_HangHoa_Anh hha
	INNER JOIN @tblResult rs ON hha.ID_HangHoa = rs.ID
	WHERE hha.SoThuTu = 1
	GROUP BY hha.ID_HangHoa) a ON a.ID_HangHoa = trs.ID

	SELECT * FROM @tblResult");

            CreateStoredProcedure(name: "[dbo].[GetListNhanVienDatLichCheckin]", parametersAction: p => new
            {
                IdDonVi = p.Guid()
            }, body: @"SET NOCOUNT ON;

    -- Insert statements for procedure here
	SELECT  TOP 10 nv.ID, nv.TenNhanVien, nva.URLAnh FROM NS_NhanVien nv
	INNER JOIN NS_QuaTrinhCongTac qtct ON nv.ID = qtct.ID_NhanVien
	LEFT JOIN NS_NhanVien_Anh nva ON nva.ID_NhanVien = nv.ID
	WHERE nv.TrangThai = 0 and qtct.ID_DonVi = @IdDonVi
	ORDER BY TenNhanVien;");

            CreateStoredProcedure(name: "[dbo].[GetNhanVienDatLichChiTiet]", parametersAction: p => new
            {
                Id = p.Guid()
            }, body: @"SET NOCOUNT ON;

    -- Insert statements for procedure here
	SELECT nv.ID, nv.MaNhanVien, nv.TenNhanVien FROM CSKH_DatLich_NhanVien dlnv
	INNER JOIN NS_NhanVien nv ON nv.ID = dlnv.IDNhanVien
	WHERE dlnv.IDDatLich = @Id");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_TonKho_TongHop]
    @ID_DonVis [nvarchar](max),
    @ThoiGian [datetime],
	@SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@TonKho INT
AS
BEGIN
   SET NOCOUNT ON;
    DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER, MaDonVi nvarchar(max), TenDonVi nvarchar(max));
	INSERT INTO @tblChiNhanh 
	SELECT dv.id, dv.MaDonVi, dv.TenDonVi 
	FROM splitstring(@ID_DonVis) cn
	join DM_DonVi dv on cn.Name = dv.ID;   

	declare @tblNhomHang table(ID UNIQUEIDENTIFIER)
	

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    Case when nd.LaAdmin = '1' then '1' else
    Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    From
    HT_NguoiDung nd	
    where nd.ID = @ID_NguoiDung);

		declare @tkDauKy table (ID_DonVi uniqueidentifier,ID_HangHoa uniqueidentifier,	ID_LoHang uniqueidentifier null, TonKho float,GiaVon float)		
		insert into @tkDauKy
		exec dbo.GetAll_TonKhoDauKy @ID_DonVis, @ThoiGian	

		if @ID_NhomHang is null
		begin
			select ID AS ID_ChiNhanh,
				TenDonVi AS TenChiNhanh, 
				round(sum(TonCuoiKy),3) as SoLuong, 
				SUM(GiaTriCuoiKy) as GiaTri
			from
			(
    		SELECT dv.ID,
				dv.MaDonVi,
				dv.TenDonVi,
    			ROUND(ISNULL(tonkho.TonKho, 0), 3) AS TonCuoiKy,
    			IIF(@XemGiaVon = '1', ISNULL(tonkho.TonKho, 0) * ISNULL(tonkho.GiaVon, 0),0) AS GiaTriCuoiKy 
			FROM DM_HangHoa dhh   	
			JOIN DonViQuiDoi dvqd1 ON dhh.ID = dvqd1.ID_HangHoa		
			left JOIN DM_NhomHangHoa nhh ON  dhh.ID_NhomHang = nhh.ID 
    		LEFT JOIN DM_LoHang lh ON dhh.ID = lh.ID_HangHoa    			
			cross join @tblChiNhanh dv 
    		LEFT JOIN @tkDauKy tonkho ON dhh.ID = tonkho.ID_HangHoa AND dv.ID = tonkho.ID_DonVi
    		 AND (lh.ID = tonkho.ID_LoHang OR (tonkho.ID_LoHang IS NULL and dhh.QuanLyTheoLoHang='0')) 
    		WHERE dhh.LaHangHoa = 1 
			AND dhh.TheoDoi LIKE @TheoDoi AND dvqd1.Xoa LIKE @TrangThai AND dvqd1.LaDonViChuan = 1
			and exists (select ID from @tblChiNhanh dv2 where dv.ID= dv2.ID)
			AND 
			IIF(@TonKho = 1, ISNULL(tonkho.TonKho, 0), 1) > 0
			AND IIF(@TonKho = 2, ISNULL(tonkho.TonKho, 0), 0) <= 0
			and IIF(@TonKho = 3, isnull(tonkho.TonKho,0), -1) < 0		
    		AND ((select count(Name) from @tblSearchString b where 
    				dhh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    					or dhh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    					or dhh.TenHangHoa like '%'+b.Name+'%'
    					or lh.MaLoHang like '%' +b.Name +'%' 
    					or dvqd1.MaHangHoa like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    					or dvqd1.TenDonViTinh like '%'+b.Name+'%'
    					or dvqd1.ThuocTinhGiaTri like '%'+b.Name+'%'
						or dv.MaDonVi like '%'+b.Name+'%'
						or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
						) a
						GROUP BY a.ID, a.TenDonVi
		end
		else
		begin
			insert into @tblNhomHang
			SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) 	

			select ID AS ID_ChiNhanh,
				TenDonVi AS TenChiNhanh, 
				round(sum(TonCuoiKy),3) as SoLuong, 
				SUM(GiaTriCuoiKy) as GiaTri
			from
			(
    		SELECT dv.ID,
				dv.MaDonVi,
				dv.TenDonVi,
    			ROUND(ISNULL(tonkho.TonKho, 0), 3) AS TonCuoiKy,
    			IIF(@XemGiaVon = '1', ISNULL(tonkho.TonKho, 0) * ISNULL(tonkho.GiaVon, 0),0) AS GiaTriCuoiKy 
			FROM DM_HangHoa dhh   	
			JOIN DonViQuiDoi dvqd1 ON dhh.ID = dvqd1.ID_HangHoa		
			left JOIN DM_NhomHangHoa nhh ON  dhh.ID_NhomHang = nhh.ID 
    		LEFT JOIN DM_LoHang lh ON dhh.ID = lh.ID_HangHoa    			
			cross join @tblChiNhanh dv 
    		LEFT JOIN @tkDauKy tonkho ON dhh.ID = tonkho.ID_HangHoa AND dv.ID = tonkho.ID_DonVi
    		 AND (lh.ID = tonkho.ID_LoHang OR (tonkho.ID_LoHang IS NULL and dhh.QuanLyTheoLoHang='0')) 
    		WHERE dhh.LaHangHoa = 1 
			AND dhh.TheoDoi LIKE @TheoDoi AND dvqd1.Xoa LIKE @TrangThai AND dvqd1.LaDonViChuan = 1
			and exists (select ID from @tblChiNhanh dv2 where dv.ID= dv2.ID)
			and exists (SELECT ID FROM @tblNhomHang allnhh where nhh.ID = allnhh.ID)	
			AND 
			IIF(@TonKho = 1, ISNULL(tonkho.TonKho, 0), 1) > 0
			AND IIF(@TonKho = 2, ISNULL(tonkho.TonKho, 0), 0) <= 0
			and IIF(@TonKho = 3, isnull(tonkho.TonKho,0), -1) < 0		
    		AND ((select count(Name) from @tblSearchString b where 
    				dhh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    					or dhh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    					or dhh.TenHangHoa like '%'+b.Name+'%'
    					or lh.MaLoHang like '%' +b.Name +'%' 
    					or dvqd1.MaHangHoa like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    					or nhh.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    					or dvqd1.TenDonViTinh like '%'+b.Name+'%'
    					or dvqd1.ThuocTinhGiaTri like '%'+b.Name+'%'
						or dv.MaDonVi like '%'+b.Name+'%'
						or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
						) a
						GROUP BY a.ID, a.TenDonVi
		end

		
END");

			Sql(@"ALTER PROCEDURE [dbo].[getList_TongQuanThuChi]
    @ID_ChiNhanh [uniqueidentifier],
    @timeStart [datetime],
    @timeEnd [datetime]
AS
BEGIN

SELECT
CAST(ROUND(SUM(k.TienThu_Thu), 0) as float) as TienThu_Thu,
CAST(ROUND(SUM(k.TienMat_Thu), 0) as float) as TienMat_Thu,
CAST(ROUND(SUM(k.NganHang_Thu), 0) as float) as NganHang_Thu,
CAST(ROUND(SUM(k.TienThu_Chi), 0) as float) as TienThu_Chi,
CAST(ROUND(SUM(k.TienMat_Chi), 0) as float) as TienMat_Chi,
CAST(ROUND(SUM(k.NganHang_Chi), 0) as float) as NganHang_Chi,
CAST(ROUND(SUM(k.ThuNo_Tong), 0) as float) as ThuNo_Tong,
CAST(ROUND(SUM(k.ThuNo_Mat), 0) as float) as ThuNo_Mat,
CAST(ROUND(SUM(k.ThuNo_NganHang), 0) as float) as ThuNo_NganHang
FROM
(
    Select 
	0 as TienThu_Chi,
	0 as TienMat_Chi,
	0 as NganHang_Chi,
	sum(qct.TienThu) as TienThu_Thu,
	SUM(iif(qct.HinhThucThanhToan=1, qct.TienThu,0)) as TienMat_Thu, 
	SUM(iif(qct.HinhThucThanhToan=2 or qct.HinhThucThanhToan =3 , qct.TienThu,0)) as NganHang_Thu,
	--SUM(TienMat + TienGui) as TienThu_Thu, SUM(TienMat) as TienMat_Thu, Sum(TienGui) as NganHang_Thu,
	0 as ThuNo_Tong,
	0 as ThuNo_Mat,
	0 as ThuNo_NganHang
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
	where qhd.NgayLapHoaDon >= @timeStart and qhd.NgayLapHoaDon < @timeEnd
	and qhd.ID_DonVi = @ID_ChiNhanh
	and qhd.LoaiHoaDon = 11
	and (qhd.PhieuDieuChinhCongNo != 1 or qhd.PhieuDieuChinhCongNo is null)
	and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
	and qct.HinhThucThanhToan not in (4,5,6)

	Union all -- tiền chi
	select 
	sum(qct.TienThu) as TienThu_Chi,
	SUM(iif(qct.HinhThucThanhToan=1, qct.TienThu,0)) as TienMat_Chi, 
	SUM(iif(qct.HinhThucThanhToan=2 or qct.HinhThucThanhToan =3 , qct.TienThu,0)) as TienThu_Chi,
	0 as TienThu_Thu,
	0 as TienMat_Thu,
	0 as NganHang_Thu,
	0,
	0,
	0
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
	where qhd.NgayLapHoaDon >= @timeStart and qhd.NgayLapHoaDon < @timeEnd
	and qhd.ID_DonVi = @ID_ChiNhanh
	and qhd.LoaiHoaDon = 12
	and (qhd.PhieuDieuChinhCongNo != 1  OR qhd.PhieuDieuChinhCongNo is null)
	and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
	and qct.HinhThucThanhToan not in (4,5,6)

	Union all -- thu congno khach
	select 
	0,
	0,
	0,	
	0 as TienThu_Thu,
	0 as TienMat_Thu,
	0 as NganHang_Thu,
	sum(qct.TienThu) as ThuNo_Tong,
	SUM(iif(qct.HinhThucThanhToan=1, qct.TienThu,0)) as ThuNo_Mat, 
	SUM(iif(qct.HinhThucThanhToan=2 or qct.HinhThucThanhToan =3 , qct.TienThu,0)) as ThuNo_NganHang
	from Quy_HoaDon_ChiTiet qct
	join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID
	join BH_HoaDon hd on qct.ID_HoaDonLienQuan = hd.ID
	where qhd.NgayLapHoaDon >= @timeStart and qhd.NgayLapHoaDon < @timeEnd
	and qhd.ID_DonVi = @ID_ChiNhanh
	and qhd.LoaiHoaDon = 11
	and (qhd.PhieuDieuChinhCongNo != 1  OR qhd.PhieuDieuChinhCongNo is null)
	and (qhd.TrangThai != '0' OR qhd.TrangThai is null)
	and hd.LoaiHoaDon in (1,19)
	and qct.HinhThucThanhToan not in (4,5,6)
	and hd.ChoThanhToan= 0 
	and hd.NgayLapHoaDon < @timeStart
) as k
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetNoKhachHang_byDate]
    @ID_DoiTuong [uniqueidentifier],
    @ToDate [nvarchar](20) ,
	@IDChiNhanhs nvarchar(max) = null
AS
BEGIN

    set nocount on

	declare @LoaiDoiTuong int= (select top 1 LoaiDoiTuong from DM_DoiTuong where ID = @ID_DoiTuong)

	declare @tblChiNhanh table (ID uniqueidentifier)
	declare @tblHDBan table (ID_DoiTuong uniqueidentifier, DoanhThu float)

	if ISNULL(@IDChiNhanhs,'')=''
		begin
			---- all chinhanh
			insert into @tblChiNhanh
			select ID from DM_DonVi where TrangThai is null or TrangThai= 1
		end
	else
		begin
			insert into @tblChiNhanh
			select name from dbo.splitstring(@IDChiNhanhs)
		end

	if @LoaiDoiTuong= 3
	begin
			insert into @tblHDBan
			SELECT 
    			bhd.ID_BaoHiem,							   					
				sum(isnull(bhd.PhaiThanhToanBaoHiem,0)) AS DoanhThu   									
    		FROM BH_HoaDon bhd
			join @tblChiNhanh cn on bhd.ID_DonVi= cn.ID					
    		WHERE bhd.ID_BaoHiem = @ID_DoiTuong
    			AND bhd.LoaiHoaDon in (1,7,19,22,25) 
				and bhd.ChoThanhToan = 0
    			AND bhd.NgayLapHoaDon < @ToDate
    		GROUP BY bhd.ID_BaoHiem
	end
	else
		begin
			insert into @tblHDBan
			SELECT 
    			bhd.ID_DoiTuong,							   					
				sum(isnull(bhd.PhaiThanhToan,0)) AS DoanhThu   					
    		FROM BH_HoaDon bhd
			join @tblChiNhanh cn on bhd.ID_DonVi= cn.ID					
    		WHERE bhd.ID_DoiTuong = @ID_DoiTuong
    			AND bhd.LoaiHoaDon in (1,7,19,22,25) 
				and bhd.ChoThanhToan = 0
    			AND bhd.NgayLapHoaDon < @ToDate
    		GROUP BY bhd.ID_DoiTuong
		end

    	select dt.ID, dt.MaDoiTuong, dt.TenDoiTuong,
    		ISNULL(tbl.NoHienTai,0) as NoHienTai
    	from     	
    		(
    			SELECT
    			td.ID_DoiTuong ,
    			SUM(ISNULL(td.DoanhThu,0)) + SUM(ISNULL(td.TienChi,0)) - SUM(ISNULL(td.TienThu,0)) 
				- SUM(isnull(td.PhiDichVu,0))  - SUM(ISNULL(td.GiaTriTra,0)) AS NoHienTai,
    			SUM(ISNULL(td.DoanhThu,0))- SUM(ISNULL(td.GiaTriTra,0)) AS TongBanTruTraHang,
    			SUM(ISNULL(td.DoanhThu,0)) AS TongMua
    
    			FROM
    			(
				---- chiphi DV ngoai
					select 
							cp.ID_NhaCungCap as ID_DoiTuong,
							0 as GiaTriTra,
    						0 as DoanhThu,
							0 AS TienThu,
    						0 AS TienChi,    					
							sum(cp.ThanhTien) as PhiDichVu
						from BH_HoaDon_ChiPhi cp
						join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
						join @tblChiNhanh cn on hd.ID_DonVi= cn.ID
						where hd.ChoThanhToan = 0
						and cp.ID_NhaCungCap = @ID_DoiTuong
						and hd.NgayLapHoaDon < @ToDate
						group by cp.ID_NhaCungCap
						union all
				
				---- tongban
    				SELECT 
    					ID_DoiTuong,							
    					0 AS GiaTriTra,
    					DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi,
						0 as PhiDichVu
    				FROM @tblHDBan
    				
    				UNION All

					-- tongtra
    				SELECT bhd.ID_DoiTuong,						
    					SUM(ISNULL(bhd.PhaiThanhToan,0)) AS GiaTriTra,
    					0 AS DoanhThu,
    					0 AS TienThu,
    					0 AS TienChi,
						0 as PhiDichVu
    				FROM BH_HoaDon bhd
					join @tblChiNhanh cn on bhd.ID_DonVi= cn.ID
    				WHERE bhd.ID_DoiTuong= @ID_DoiTuong
						AND bhd.LoaiHoaDon in (4,6) 
						and bhd.ChoThanhToan = 0    					
    					AND bhd.NgayLapHoaDon < @ToDate
    				GROUP BY bhd.ID_DoiTuong
    					
    				UNION ALL

					-- tienthu
    				SELECT 
    					qhdct.ID_DoiTuong,							
    					0 AS GiaTriTra,
    					0 AS DoanhThu,
    					SUM(ISNULL(qhdct.TienThu,0)) AS TienThu,
    					0 AS TienChi,
						0 as PhiDichVu
    				FROM Quy_HoaDon qhd
					join @tblChiNhanh cn on qhd.ID_DonVi= cn.ID
    				JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon    				
    				WHERE qhdct.ID_DoiTuong= @ID_DoiTuong
    					AND qhd.LoaiHoaDon = 11 AND  (qhd.TrangThai != 0 OR qhd.TrangThai is null)
    					AND qhd.NgayLapHoaDon < @ToDate
    					and qhdct.HinhThucThanhToan!=6
						AND (qhdct.LoaiThanhToan is null OR qhdct.LoaiThanhToan !=3)
    				GROUP BY qhdct.ID_DoiTuong
    
    				UNION ALL

					----tienchi
    				SELECT 
    					qhdct.ID_DoiTuong,							
    					0 AS GiaTriTra,
    					0 AS DoanhThu,
    					0 AS TienThu,
    					SUM(ISNULL(qhdct.TienThu,0)) AS TienChi,
						0 as PhiDichVu
    				FROM Quy_HoaDon qhd
					join @tblChiNhanh cn on qhd.ID_DonVi= cn.ID
    				JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				WHERE qhdct.ID_DoiTuong= @ID_DoiTuong
    					AND qhd.LoaiHoaDon = 12 AND (qhd.TrangThai != 0 OR qhd.TrangThai is null)
    					AND qhd.NgayLapHoaDon < @ToDate
						AND (qhdct.LoaiThanhToan is null OR qhdct.LoaiThanhToan !=3)
    				GROUP BY qhdct.ID_DoiTuong	
				
    		) AS td GROUP BY td.ID_DoiTuong
			
    	) tbl
		join DM_DoiTuong dt on tbl.ID_DoiTuong = dt.ID
END");

			Sql(@"ALTER PROCEDURE [dbo].[TheGiaTri_GetLichSuNapTien]
    @IDChiNhanhs [nvarchar](max),
    @ID_Cutomer [nvarchar](max),
    @TextSearch [nvarchar](max) = null,
    @DateFrom [nvarchar](max) = null,
    @DateTo [nvarchar](max) = null,
    @CurrentPage [int] = null,
    @PageSize [int] = null
AS
BEGIN
    SET NOCOUNT ON;
    	
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
    		set @whereIn = ' where 1 = 1 and hd.LoaiHoaDon in (22,23) and hd.ChoThanhToan = 0'
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
					hd.TongTienHang as SoDuSauNap,
					hd.DienGiai
    				into #htThe
    			from BH_HoaDon hd ', @whereIn)
    			   
    		
    			set @sql2 = concat(N'
				---- get luyke soduSauNap
				declare @soduLuyke float = 0
				declare @ID_HoaDon uniqueidentifier, @SoDuSauNap float
				declare _cur cursor for

				select ID, SoDuSauNap
				from #htThe order by NgayLapHoaDon 
	
				open _cur
				FETCH NEXT FROM _cur
				INTO @ID_HoaDon, @SoDuSauNap
				WHILE @@FETCH_STATUS = 0
				BEGIN   
					 set @soduLuyke = @soduLuyke + @SoDuSauNap
					 update #htThe set SoDuSauNap= @soduLuyke where ID= @ID_HoaDon		
					FETCH NEXT FROM _cur

				INTO @ID_HoaDon, @SoDuSauNap

				END
				CLOSE _cur;
				DEALLOCATE _cur;

    	
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
    					hd.SoDuSauNap,
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
    			', @whereOut , ' order by tbl.NgayLapHoaDon desc')
    
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
    			@PageSize_In = @PageSize
END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateLichBaoDuong_whenUpdateSoKM_ofPhieuTN]
    @ID_PhieuTiepNhan [uniqueidentifier],
    @ChenhLech_SoKM [float] --- not use
AS
BEGIN
    SET NOCOUNT ON;
		
	declare @ID_HoaDon uniqueidentifier
	declare _cur cursor for

	select distinct lich.ID_HoaDon
	from Gara_LichBaoDuong lich 
	join BH_HoaDon hd on lich.ID_HoaDon= hd.ID
    where hd.ID_PhieuTiepNhan= @ID_PhieuTiepNhan
    and lich.SoKmBaoDuong > 0
    and lich.TrangThai= 1


	open _cur
	FETCH NEXT FROM _cur
	INTO @ID_HoaDon
	WHILE @@FETCH_STATUS = 0
	BEGIN   
		 exec Insert_LichNhacBaoDuong @ID_HoaDon
		FETCH NEXT FROM _cur

	INTO @ID_HoaDon

	END
	CLOSE _cur;
	DEALLOCATE _cur;
END");

            Sql(@"ALTER PROCEDURE [dbo].[UpdateTonKho_multipleDVT]
    @isUpdate [int],
    @ID_DonVi [uniqueidentifier],
    @ID_DonViQuyDoi [uniqueidentifier],
    @ID_LoHang [uniqueidentifier],
    @TonKho float
AS
BEGIN
    SET NOCOUNT ON;
    
    	if @isUpdate = 2
    	begin
    		---- get infor donviquidoi by ID
    		declare @ID_HangHoa uniqueidentifier, @TyLeChuyenDoi float, @LaDonViChuan bit
    		select @ID_HangHoa = ID_HangHoa, 
    			@TyLeChuyenDoi= iif(TyLeChuyenDoi is null or TyLeChuyenDoi =0,1, TyLeChuyenDoi), 
    			@LaDonViChuan= LaDonViChuan
    		from DonViQuiDoi where id= @ID_DonViQuyDoi
    
    		--- get infor hanghoa
    		declare @QuanLyTheoLo bit, @LaHangHoa bit
    		select @QuanLyTheoLo = QuanLyTheoLoHang, @LaHangHoa = LaHangHoa from DM_HangHoa where ID = @ID_HangHoa
    
    		---- get all dvt
    		select ID, iif(TyLeChuyenDoi is null or TyLeChuyenDoi =0,1, TyLeChuyenDoi) as TyLeChuyenDoi,LaDonViChuan
    		into #allDVT
    		from DonViQuiDoi where ID_HangHoa = @ID_HangHoa
    
    		if @LaHangHoa='0'
    			begin
    			--- reset tonkho if change hanghoa --> dichvu
    				update tk  set TonKho = 0 
    				from DM_HangHoa_TonKho tk
    				where exists (select ID from #allDVT qd where tk.ID_DonViQuyDoi = qd.ID)
    			end
    		else
    		begin
    			declare @cur_ID_DonViQuiDoi uniqueidentifier, @cur_TyLeChuyenDoi float
    			if @LaDonViChuan ='1'
    			begin
    				declare _cur cursor for
    				select ID, TyLeChuyenDoi from #allDVT
    				open _cur
    				fetch next from _cur into @cur_ID_DonViQuiDoi, @cur_TyLeChuyenDoi
    				while @@FETCH_STATUS =0
    				begin
    					--- update tonkho for dvt #
    					update DM_HangHoa_TonKho set TonKho = @TonKho / @cur_TyLeChuyenDoi
    					where ID_DonVi = @ID_DonVi and ID_DonViQuyDoi = @cur_ID_DonViQuiDoi
    					and (@QuanLyTheoLo='0' or @QuanLyTheoLo is null or ID_LoHang= @ID_LoHang)
    
    					fetch next from _cur
    					into @cur_ID_DonViQuiDoi, @cur_TyLeChuyenDoi
    				end
    				close _cur
    				deallocate _cur
    			end
    			else
    			begin
    				declare @IDQuiDoiChuan uniqueidentifier 
    				select @IDQuiDoiChuan = ID from #allDVT where LaDonViChuan='1'
    
    				declare @tonkhoAll float = @TonKho * @TyLeChuyenDoi 
    				---- update tonkho for dvt chuan
    				update DM_HangHoa_TonKho set TonKho = @tonkhoAll
    				where ID_DonVi = @ID_DonVi and ID_DonViQuyDoi = @IDQuiDoiChuan
    					and (@QuanLyTheoLo='0' or @QuanLyTheoLo is null or ID_LoHang= @ID_LoHang)
    
    				---- update tokho for dvt # (vd: hang co 3 dvt tro len)
    				declare _cur cursor for
    				select ID, TyLeChuyenDoi from #allDVT where ID != @IDQuiDoiChuan and ID != @ID_DonViQuyDoi
    				open _cur
    				fetch next from _cur into @cur_ID_DonViQuiDoi, @cur_TyLeChuyenDoi
    				while @@FETCH_STATUS =0
    				begin					
    					update DM_HangHoa_TonKho set TonKho = @tonkhoAll / @cur_TyLeChuyenDoi
    					where ID_DonVi = @ID_DonVi and ID_DonViQuyDoi = @cur_ID_DonViQuiDoi
    					and (@QuanLyTheoLo='0' or @QuanLyTheoLo is null or ID_LoHang= @ID_LoHang)
    
    					fetch next from _cur
    					into @cur_ID_DonViQuiDoi, @cur_TyLeChuyenDoi
    				end
    				close _cur
    				deallocate _cur
    			end		
    		end					
    	end
END");

            Sql(@"ALTER PROCEDURE [dbo].[UpdateLichBD_whenChangeNgayLapHD]
    @ID_HoaDon [uniqueidentifier],
    @NgayLapHDOld [datetime],
    @NgayLapNew [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    declare @chenhlech int= DATEDIFF(day, @NgayLapHDOld, @NgayLapNew)
    	if @chenhlech!=0
    		begin
    			
    			--- update if lich exist
    			update lich set lich.NgayBaoDuongDuKien= DATEADD(day, @chenhlech,lich.NgayBaoDuongDuKien)			
    			from Gara_LichBaoDuong lich
    			where lich.ID_HoaDon= @ID_HoaDon
    			and lich.TrangThai = 1
    			    
    		end
END");

        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[GetHangHoaDatLichChiTiet]");
            DropStoredProcedure("[dbo].[GetListDatLich]");
            DropStoredProcedure("[dbo].[GetListHangHoaDatLichCheckin]");
            DropStoredProcedure("[dbo].[GetListNhanVienDatLichCheckin]");
            DropStoredProcedure("[dbo].[GetNhanVienDatLichChiTiet]");
        }
    }
}
