namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20231030 : DbMigration
    {
        public override void Up()
        {
            CreateStoredProcedure(name: "[dbo].[CTHD_GetAllDonViTinhOfhangHoa]", parametersAction: p => new
            {
                IDHangHoas = p.String()
            }, body: @"SET NOCOUNT ON;

	declare @tblIDHangHoa table (ID uniqueidentifier primary key)
	if(isnull(@IDHangHoas,'')!='')
		insert into @tblIDHangHoa
		select distinct name from dbo.splitstring(@IDHangHoas)

	
	select 
		qd.ID,
		qd.ID_HangHoa,
		qd.TenDonViTinh,
		qd.Xoa,
		qd.TyLeChuyenDoi
	from DonViQuiDoi qd
	where exists (select id from @tblIDHangHoa hh where qd.ID_HangHoa = hh.ID)
	and qd.Xoa ='0'");

			CreateStoredProcedure(name: "[dbo].[CTHD_GetAllNhanVienThucHien]", parametersAction: p => new
			{
				IDChiTiets = p.String()
			}, body: @"SET NOCOUNT ON;

	declare @tblIDChiTietHD table (ID uniqueidentifier primary key)
	if(isnull(@IDChiTiets,'')!='')
		insert into @tblIDChiTietHD
		select name from dbo.splitstring(@IDChiTiets)


	select 
		th.ID_NhanVien,
		th.ID_ChiTietHoaDon,
		th.ThucHien_TuVan,
		th.TienChietKhau,
		th.PT_ChietKhau,
		th.TheoYeuCau,
		th.HeSo,
		th.TinhChietKhauTheo,
		ISNULL(th.TinhHoaHongTruocCK,0) as TinhHoaHongTruocCK,
		nv.TenNhanVien
	from BH_NhanVienThucHien th
	join NS_NhanVien nv on th.ID_NhanVien= nv.ID
	where exists (select ID from @tblIDChiTietHD ct where th.ID_ChiTietHoaDon = ct.ID)");

			Sql(@"ALTER TRIGGER [dbo].[UpdateNgayGiaoDichGanNhat_DMDoiTuong]
   ON [dbo].[BH_HoaDon]
   after insert, update
AS 
BEGIN

	SET NOCOUNT ON;
	declare @ID_KhachHang uniqueidentifier, @NgayInsert DATETIME, @LoaiHoaDon INT, @ChoThanhToan INT;
	select top 1 @ID_KhachHang = ID_DoiTuong, @NgayInsert = NgayLapHoaDon, @LoaiHoaDon = LoaiHoaDon, @ChoThanhToan = ChoThanhToan from inserted;
	DECLARE @NgayMaxTemp datetime;
	SELECT @NgayMaxTemp = NgayGiaoDichGanNhat FROM DM_DoiTuong where ID = @ID_KhachHang
	IF (@NgayInsert > @NgayMaxTemp or @NgayMaxTemp is null) AND @LoaiHoaDon IN (1,2,19,22,25,36) AND @ChoThanhToan IS NOT NULL
	BEGIN
		update dt set NgayGiaoDichGanNhat = @NgayInsert -- inserted.NgayLapHoaDon
		from DM_DoiTuong dt
		where ID = @ID_KhachHang
	END
END");

            Sql(@"ALTER PROCEDURE [dbo].[GetListPhieuTiepNhan_v2]
    @IdChiNhanhs [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
	@IdUserLogin uniqueidentifier = null,
    @NgayTiepNhan_From [datetime] =null,
    @NgayTiepNhan_To [datetime]= '2025-01-01',
    @NgayXuatXuongDuKien_From [datetime]=null,
    @NgayXuatXuongDuKien_To [datetime] ='2024-01-01',
    @NgayXuatXuong_From [datetime]=null,
    @NgayXuatXuong_To [datetime] ='2024-01-01',
    @TrangThais [nvarchar](20)='1,2,0,3',
    @TextSearch [nvarchar](max)='30A72074',
    @CurrentPage [int]=0,
    @PageSize [int]= 0,
	@BaoHiem int = 3
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
    	if(@PageSize != 0)
    	BEGIN
    		with data_cte
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
    		),
			tblView
			as(
				SELECT dt.*, ct.*,
					row_number() over (partition by dt.ID_Xe order by dt.NgayVaoXuong desc) as RN1
				FROM data_cte dt
				CROSS JOIN count_cte ct
				ORDER BY dt.NgayVaoXuong desc
				OFFSET (@CurrentPage * @PageSize) ROWS
				FETCH NEXT @PageSize ROWS ONLY
			),
			ptnGanNhat 
			as (
				---- get soKm cu gan nhat theo tung PTN ----
				select tn.ID_Xe, iif(tn.SoKmRa = 0 or tn.SoKmRa = 0, tn.SoKmVao, tn.SoKmRa) as SoKmCu,
					row_number() over (partition by tn.ID_Xe order by NgayVaoXuong desc) as RN2
				from Gara_PhieuTiepNhan tn			
				where tn.trangThai !=0  
				and exists (select * from  data_cte dt where tn.ID_Xe = dt.ID_Xe and tn.NgayVaoXuong < dt.NgayVaoXuong)
			)
			select t1.*, iif(isnull(t2.SoKmCu,0) =0, t1.SoKmVao, t2.SoKmCu) as SoKmCu
			from tblView t1 
			left join ptnGanNhat t2 on t1.ID_Xe= t2.ID_Xe and t1.RN1 = t2.RN2
    	END
    	ELSE
    	BEGIN
    		with data_cte
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
    			),
				tblView
				as(
					SELECT dt.*,
						row_number() over (partition by dt.ID_Xe order by dt.NgayVaoXuong desc) as RN1
					FROM data_cte dt					
				),
    			ptnGanNhat 
				as (
					---- get soKm cu gan nhat theo tung PTN ----
					select tn.ID_Xe, iif(tn.SoKmRa = 0 or tn.SoKmRa = 0, tn.SoKmVao, tn.SoKmRa) as SoKmCu,
						row_number() over (partition by tn.ID_Xe order by NgayVaoXuong desc) as RN2
					from Gara_PhieuTiepNhan tn			
					where tn.trangThai !=0  
					and exists (select * from  data_cte dt where tn.ID_Xe = dt.ID_Xe and tn.NgayVaoXuong < dt.NgayVaoXuong)
				)
				select t1.*,
					iif(isnull(t2.SoKmCu,0) =0, t1.SoKmVao, t2.SoKmCu),
					1 as TotalRow, ---- avoid error at c#
					CAST(1 AS FLOAT) AS TotalPage
				from tblView t1 
				left join ptnGanNhat t2 on t1.ID_Xe= t2.ID_Xe and t1.RN1 = t2.RN2
    	END
END");

            Sql(@"ALTER PROCEDURE [dbo].[BCBanHang_GetCTHD]
    @IDChiNhanhs [nvarchar](max),
    @DateFrom [datetime],
    @DateTo [datetime],
    @LoaiChungTus [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    
    	
    	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER)
    INSERT INTO @tblChiNhanh
    select Name from splitstring(@IDChiNhanhs);
    
    	DECLARE @tblLoaiHoaDon TABLE(LoaiHoaDon int)
    INSERT INTO @tblLoaiHoaDon
    select Name from splitstring(@LoaiChungTus);
    
    
    	--- hdmua
    	select 
    		hd.NgayLapHoaDon, hd.MaHoaDon,hd.LoaiHoaDon,
    		hd.ID_DonVi, hd.ID_PhieuTiepNhan, hd.ID_DoiTuong, hd.ID_NhanVien,	
    		hd.TongTienHang, hd.TongGiamGia,hd.KhuyeMai_GiamGia,
    		hd.ChoThanhToan,
    		ct.ID, ct.ID_HoaDon, ct.ID_DonViQuiDoi, ct.ID_LoHang,
    		ct.ID_ChiTietGoiDV, ct.ID_ChiTietDinhLuong, ct.ID_ParentCombo,
    		ct.SoLuong, ct.DonGia,  ct.GiaVon,
    		ct.TienChietKhau, ct.TienChiPhi,
    		ct.ThanhTien, ct.ThanhToan,
    		ct.GhiChu, ct.ChatLieu,
    		ct.LoaiThoiGianBH, ct.ThoiGianBaoHanh,		
			ct.TenHangHoaThayThe,
    		Case when hd.TongTienThueBaoHiem > 0 
    			then case when hd.TongThueKhachHang = 0 or hd.TongThueKhachHang is null
    				then ct.ThanhTien * (hd.TongTienThue / hd.TongTienHang) 
    				else iif(hd.TongThueKhachHang > 0 and ct.TienThue = 0, ct.ThanhTien * (hd.TongTienThue / hd.TongTienHang),ct.TienThue * ct.SoLuong) end
    		else ct.TienThue * ct.SoLuong end as TienThue,
    		Case when hd.TongTienHang = 0 then 0 else ct.ThanhTien * ((hd.TongGiamGia + isnull(hd.KhuyeMai_GiamGia,0)) / hd.TongTienHang) end as GiamGiaHD,
			Case when hd.TongTienHang = 0 then 0 else ct.ThanhTien *  isnull(hd.GiamTruThanhToanBaoHiem,0) / hd.TongTienHang end as GiamTruThanhToanBaoHiem
    	into #cthdMua
    	from BH_HoaDon_ChiTiet ct
    	join BH_HoaDon hd on ct.ID_HoaDon = hd.ID	
    where hd.ChoThanhToan=0
    and hd.NgayLapHoaDon between @DateFrom and @DateTo
    and exists (select ID from @tblChiNhanh cn where cn.ID = hd.ID_DonVi)
    and exists (select LoaiHoaDon from @tblLoaiHoaDon loai where loai.LoaiHoaDon = hd.LoaiHoaDon)
	and hd.LoaiHoaDon!=6
    and (ct.ChatLieu is null or ct.ChatLieu not in ('4','5'))
    
    
    	---- === GIA VON HD LE ===
    	select 
    		b.IDComBo_Parent,
    		sum(b.GiaVon) as GiaVon,
    		sum(b.TienVon) as TienVon
    	INTO #gvLe
    	from
    	(
    	 select dluongParent.*,
    			 iif(ctm.ID_ParentCombo is not null, ctm.ID_ParentCombo, ctm.ID) as IDComBo_Parent
    		 from
    		 (
    	select 
    		---- khong get ID_ChiTietGoiDV neu xu ly dathang
    		iif(ctAll.ID_ChiTietGoiDV is not null and ctAll.ID_HoaDon is null, ctAll.ID_ChiTietGoiDV,ctAll.ID) as ID_ChiTietGoiDV,
    		child.GiaVon,
    		child.TienVon
    		from
    		(
    			select 
    				ct.ID_ComBo,
    				sum(ct.GiaVon) as GiaVon,
    				sum(ct.TienVon) as TienVon
    			from
    			(
    			select 
    				iif(ctLe.ID_ParentCombo is not null, ctLe.ID_ParentCombo, 
    								iif(ctLe.ID_ChiTietDinhLuong is not null, ctLe.ID_ChiTietDinhLuong, ctLe.ID)) as ID_ComBo,
    				iif(ctLe.ID_ParentCombo = ctLe.ID or ctLe.ID_ChiTietDinhLuong = ctLe.ID, 0,  ctLe.GiaVon) as GiaVon,
    				iif(ctLe.ID_ParentCombo = ctLe.ID or ctLe.ID_ChiTietDinhLuong = ctLe.ID, 0, ctLe.SoLuong * ctLe.GiaVon) as TienVon
    			from #cthdMua ctLe
    			where LoaiHoaDon= 1 
    			) ct group by ct.ID_ComBo
    		) child
    		join #cthdMua ctAll on child.ID_ComBo = ctAll.ID
    		) dluongParent join #cthdMua ctm on dluongParent.ID_ChiTietGoiDV= ctm.ID
    	) b group by b.IDComBo_Parent
    	
    
    ---- xuatkho or sudung gdv
    select hdx.MaHoaDon, hdx.LoaiHoaDon,
    	ctx.ID,	ctx.ID_ChiTietDinhLuong, ctx.ID_ParentCombo,ctx.ID_ChiTietGoiDV,
    	ctx.ID_DonViQuiDoi,
    	ctx.SoLuong, ctx.GiaVon, ctx.ThanhTien
    	into #tblAll
    from BH_HoaDon_ChiTiet ctx
    join BH_HoaDon hdx on ctx.ID_HoaDon= hdx.ID
    	   where hdx.ChoThanhToan = 0 AND exists (
    	   select ID
    	   from #cthdMua ctm where ctx.ID_ChiTietGoiDV = ctm.ID
    )
    
    select xksdGDV.ID_ChiTietGoiDV, xksdGDV.GiaVon, xksdGDV.SoLuong  *  xksdGDV.GiaVon as TienVon
    into #xksdGDV
    from BH_HoaDon_ChiTiet xksdGDV
    where exists (
    	   select ID
    	   from #tblAll ctsc where xksdGDV.ID_ChiTietGoiDV = ctsc.ID)
    
    
    				---- === GIAVON XUATKHO SUA CHUA ===
    	
    				select 
    					c.ID_Parent,
    					sum(c.GiaVon) as GiaVon,
    					sum(c.TienVon) as TienVon
    				into  #xuatSC
    				from
    				(
    				select 
    					iif(ctm2.ID_ParentCombo is not null, ctm2.ID_ParentCombo, ctm2.ID) as ID_Parent,
    					b.GiaVon,
    					b.TienVon
    				from
    				(
    				select 
    					gvXK.ID_Combo,
    					sum(gvXK.GiaVon) as GiaVon,
    					sum(gvXK.TienVon) as TienVon			
    				from
    				(
    				select 
    					IIF(ctm.ID_ParentCombo is not null, ctm.ID_ParentCombo,
    					iif(ctm.ID_ChiTietDinhLuong is not null, ctm.ID_ChiTietDinhLuong, ctm.ID)) as ID_Combo,
    					b.GiaVon,
    					b.TienVon
    				from
    				(		
    				   select 
    					gvComBo.ID_ChiTietGoiDV,
    					sum(GiaVon) as GiaVon,
    					sum(TienVon) as TienVon
    				
    				   from
    				   (
    				   select 
    						iif(ctXuat.ID_ChiTietGoiDV is not null, ctXuat.ID_ChiTietGoiDV, ctXuat.ID) as ID_ChiTietGoiDV,
    						0 as GiaVon,
    						ctXuat.SoLuong * ctXuat.GiaVon as TienVon
    					   from #tblAll ctXuat
    					   where ctXuat.LoaiHoaDon = 8
    					) gvComBo group by gvComBo.ID_ChiTietGoiDV
    				) b
    				join #cthdMua ctm on b.ID_ChiTietGoiDV= ctm.ID
    				) gvXK 
    				group by gvXK.ID_Combo
    				) b
    				join #cthdMua ctm2 on b.ID_Combo = ctm2.ID
    				) c group by c.ID_Parent
    				
    
    
    				----  === GIAVON XUAT SUDUNG ===
    		select gvSD.IDComBo_Parent,
    			sum(gvSD.GiaVon) as GiaVon,
    			sum(gvSD.TienVon) as TienVon
    		into #gvSD
    		from
    		(
    			 ---- group combo at parent
    			 select 
    				iif(ctm2.ID_ParentCombo is not null, ctm2.ID_ParentCombo, ctm2.ID) as IDComBo_Parent,
    				b.GiaVon,
    				b.TienVon
    			 from(
    					select c.*,
    						iif(ctm.ID_ChiTietDinhLuong is not null, ctm.ID_ChiTietDinhLuong, ctm.ID) as IDDLuong_Parent
    					from
    					(
    					---- group dinhluong at parent by id_ctGoiDV
    						select 
    						iif(ctAll.ID_ChiTietGoiDV is not null, ctAll.ID_ChiTietGoiDV,ctAll.ID) as ID_ChiTietGoiDV,
    						child.GiaVon,
    						child.TienVon
    						from
    						(
    						
    							---- xuat sudung gdv le
    							select 
    								gvComBo.ID_ComBo,
    								sum(GiaVon) as GiaVon,
    								sum(TienVon) as TienVon
    							from
    							(
    							select 
    								iif(ctLe.ID_ParentCombo is not null, ctLe.ID_ParentCombo, 
    									iif(ctLe.ID_ChiTietDinhLuong is not null, ctLe.ID_ChiTietDinhLuong, ctLe.ID)) as ID_ComBo,
    								iif(ctLe.ID_ParentCombo = ctLe.ID or ctLe.ID_ChiTietDinhLuong = ctLe.ID , 0,  ctLe.GiaVon) as GiaVon,
    								iif(ctLe.ID_ParentCombo = ctLe.ID or ctLe.ID_ChiTietDinhLuong = ctLe.ID  , 0, ctLe.SoLuong * ctLe.GiaVon) as TienVon
    							from #tblAll ctLe
    							where ctLe.LoaiHoaDon in (1)
    
    							---- xuat sudung goi baoduong
    							union all
    							select *
    							from #xksdGDV
    							
    							) gvComBo group by gvComBo.ID_ComBo
    						) child
    						join #tblAll ctAll on child.ID_ComBo = ctAll.ID
    				) c join #cthdMua ctm on c.ID_ChiTietGoiDV= ctm.ID
    			) b join #cthdMua ctm2 on b.IDDLuong_Parent = ctm2.ID
    		) gvSD
    		group by gvSD.IDComBo_Parent
    	
    					
    	--select *
    	--	from #xuatSC
    
    	--	select *
    	--	from #gvSD
    
    select 
    	ctmua.*,
    	isnull(gv.GiaVon,0) as GiaVon,
    	isnull(gv.TienVon,0) as TienVon
    from #cthdMua ctmua
    left join
    (
    	
    		---- giavon hdle
    		select *
    		from #gvLe
    
    		union all
    		--- giavon xuatkho sc
    		select *
    		from #xuatSC
    
    		union all
    		--- giavon xuatkho sudung gdv
    		select *
    		from #gvSD				
    	) gv on ctmua.ID = gv.IDComBo_Parent	
END");

            Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_TongHop_Page]
    @pageNumber [int],
    @pageSize [int],
    @SearchString [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang uniqueidentifier,
	@LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@ColumnSort varchar(40)='',
	@TypeSort varchar(10)=''
AS
BEGIN
	set nocount on;
	if isnull(@ColumnSort,'')=''
		set @ColumnSort ='TenHangHoa'
	if isnull(@TypeSort,'')=''
		set @TypeSort ='ASC'

	SET @pageNumber = @pageNumber - 1; --- because @pageNumber from 1
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
			hh.ID, hh.TenHangHoa,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			qd.MaHangHoa,			
			concat(hh.TenHangHoa, qd.ThuocTinhGiaTri) as TenHangHoaFull,
			qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			ISNULL(nhh.TenNhomHangHoa,  N'Nhóm hàng hóa mặc định') as TenNhomHangHoa,
			lo.MaLoHang as TenLoHang,
			qd.TenDonViTinh,
			cast(c.SoLuong as float) as SoLuong,
			cast(c.TienChietKhau as float) as TienChietKhau,
			cast(c.ThanhTienTruocCK as float) as ThanhTienTruocCK,
			cast(c.ThanhTien as float) as ThanhTien,
			cast(c.GiamGiaHD as float) as GiamGiaHD,
			cast(c.GiamTruThanhToanBaoHiem as float) as GiamTruThanhToanBaoHiem,
			cast(c.TongTienThue as float) as TongTienThue,
			iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
			cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem as float) as DoanhThuThuan,
			iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem - c.TienVon - c.ChiPhi as float),0) as LaiLo,
			c.ChiPhi
		from 
		(
		select 
			sum(b.SoLuong * isnull(qd.TyLeChuyenDoi,1)) as SoLuong,
			sum(b.TienChietKhau) as TienChietKhau,
			sum(b.ThanhTienTruocCK) as ThanhTienTruocCK,
			sum(b.ThanhTien) as ThanhTien,
			sum(b.TienVon) as TienVon,
			qd.ID_HangHoa,
			b.ID_LoHang,			
			sum(b.GiamGiaHD) as GiamGiaHD,
			sum(b.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(b.TongTienThue) as TongTienThue	,
			sum(ChiPhi) as ChiPhi
		from (
		select 		
			a.ID_LoHang, a.ID_DonViQuiDoi,			
			sum(isnull(a.TienThue,0)) as TongTienThue,
			sum(isnull(a.GiamGiaHD,0)) as GiamGiaHD,
			sum(isnull(a.GiamTruThanhToanBaoHiem,0)) as GiamTruThanhToanBaoHiem,
			sum(SoLuong) as SoLuong,
			sum(TienChietKhau) as TienChietKhau,
			sum(ThanhTienTruocCK) as ThanhTienTruocCK,
			sum(ThanhTien) as ThanhTien,
			sum(TienVon) as TienVon,
			sum(ChiPhi) as ChiPhi
		from
		(				
		select 
			ct.ID,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			ct.TienThue,
    		ct.GiamGiaHD,
			ct.GiamTruThanhToanBaoHiem,
			ct.SoLuong,
			ct.SoLuong * ct.TienChietKhau as TienChietKhau,
			ct.SoLuong * ct.DonGia as ThanhTienTruocCK,
			ct.ThanhTien, 	
			ct.TienVon,
			isnull(cp.ChiPhi,0) as ChiPhi
		from @tblCTHD ct
		left join @tblChiPhi cp on cp.ID_ParentCombo= ct.ID
		where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)			 
			and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)	
		) a group by a.ID_LoHang, a.ID_DonViQuiDoi	
		)b
		join DonViQuiDoi qd on b.ID_DonViQuiDoi= qd.ID
		group by qd.ID_HangHoa, b.ID_LoHang					
		) c
		join DM_HangHoa hh on c.ID_HangHoa = hh.ID
		join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan=1
		left join DM_LoHang lo on c.ID_LoHang = lo.ID
		left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID		
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)		
    	and
		hh.TheoDoi like @TheoDoi
		and qd.Xoa like @TrangThai
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
			) a
			where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))

			---- sum all column
			DECLARE @Rows FLOAT,  @TongSoLuong float, @TongChietKhau float, @TongTienTruocCK float, @TongThanhTien float, @TongGiamGiaHD FLOAT, 
			@TongGiamTruBaoHiem FLOAT, 
			@TongTienVon FLOAT, @TongLaiLo FLOAT, @SumTienThue FLOAT,@TongDoanhThuThuan FLOAT,@TongChiPhi float		

			SELECT @Rows = Count(*), @TongSoLuong = SUM(SoLuong),
			@TongChietKhau = SUM(TienChietKhau),
			@TongTienTruocCK = SUM(ThanhTienTruocCK),
			@TongThanhTien = SUM(ThanhTien),
			@TongGiamGiaHD = SUM(GiamGiaHD),
			@TongGiamTruBaoHiem = SUM(GiamTruThanhToanBaoHiem),
			@TongTienVon = SUM(TienVon),
			@TongLaiLo = SUM(LaiLo),
			@SumTienThue = SUM(TongTienThue),
			@TongDoanhThuThuan = SUM(DoanhThuThuan) ,
			@TongChiPhi = SUM(ChiPhi)
			FROM #tblView;


			select *,
			@Rows as Rowns,
    		@TongSoLuong as TongSoLuong,
			@TongChietKhau as TongChietKhau,
			@TongTienTruocCK as TongTienTruocCK,
    		@TongThanhTien as TongThanhTien,
    		@TongGiamGiaHD as TongGiamGiaHD,
			@TongGiamTruBaoHiem as TongGiamTruBaoHiem,
    		@TongTienVon as TongTienVon,
    		@TongLaiLo as TongLaiLo,
			@SumTienThue as SumTienThue,
    		@TongDoanhThuThuan as TongDoanhThuThuan,
			@TongChiPhi as TongChiPhi
    		from #tblView    	
    		order by 
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenNhomHangHoa' then TenNhomHangHoa end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenNhomHangHoa' then TenNhomHangHoa end DESC,			
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='MaHangHoa' then MaHangHoa end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='MaHangHoa' then MaHangHoa end DESC,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenHangHoa' then TenHangHoa end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenHangHoa' then TenHangHoa end DESC,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenDonViTinh' then TenDonViTinh end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenDonViTinh' then TenDonViTinh end DESC,
				case when @TypeSort <>'ASC' then ''
				when @ColumnSort='TenLoHang' then TenLoHang end ASC,
				case when @TypeSort <>'DESC' then ''
				when @ColumnSort='TenLoHang' then TenLoHang end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='SoLuong' then SoLuong end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='SoLuong' then SoLuong end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='ThanhTien' then ThanhTien end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='ThanhTien' then ThanhTien end DESC	,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='DoanhThuThuan' then DoanhThuThuan end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='DoanhThuThuan' then DoanhThuThuan end DESC,	
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='TienVon' then TienVon end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='TienVon' then TienVon end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='ChiPhi' then ChiPhi end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='ChiPhi' then ChiPhi end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='LaiLo' then LaiLo end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='LaiLo' then LaiLo end DESC,				
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='GiamGiaHD' then GiamGiaHD end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='GiamGiaHD' then GiamGiaHD end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='TongTienThue' then TongTienThue end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='TongTienThue' then TongTienThue end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='TienChietKhau' then TienChietKhau end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='TienChietKhau' then TienChietKhau end DESC,
				case when @TypeSort <>'ASC' then 0
				when @ColumnSort='ThanhTienTruocCK' then ThanhTienTruocCK end ASC,
				case when @TypeSort <>'DESC' then 0
				when @ColumnSort='ThanhTienTruocCK' then ThanhTienTruocCK end DESC	
			OFFSET (@pageNumber* @pageSize) ROWS
    		FETCH NEXT @pageSize ROWS ONLY			    	
END");

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
		GiamTruThanhToanBaoHiem float,
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
			cast(c.GiamTruThanhToanBaoHiem as float) as GiamTruThanhToanBaoHiem,
			cast(c.TienThue as float) as TienThue,
			iif(@XemGiaVon='1',cast(c.GiaVon as float),0) as GiaVon,
			iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
			cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem as float) as DoanhThu,
			iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem - c.TienVon -c.ChiPhi as float),0) as LaiLo,
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
			b.GiamTruThanhToanBaoHiem,
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
			ct.TienThue,
			ct.GiamGiaHD,			
			ct.GiamTruThanhToanBaoHiem,
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
			@TongThanhTien float, @TongGiamGiaHD FLOAT,  @TongGiamTruBaoHiem FLOAT, @TongTienVon FLOAT, 
			@TongLaiLo FLOAT, @SumTienThue FLOAT,@TongDoanhThuThuan FLOAT, @TongChiPhi float			
			SELECT @Rows = Count(*), @TongSoLuong = SUM(SoLuong),
			@TongChietKhau= SUM(TienChietKhau),
			@ThanhTienTruocCK= SUM(ThanhTienTruocCK),
			@TongThanhTien = SUM(ThanhTien), @TongGiamGiaHD = SUM(GiamGiaHD),
			@TongGiamTruBaoHiem = SUM(GiamTruThanhToanBaoHiem),
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
					@TongGiamTruBaoHiem as TongGiamTruBaoHiem,
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_LoiNhuan]
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
		where nd.ID = @ID_NguoiDung);

	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
	Select @count =  (Select count(*) from @tblSearchString);

	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER)
	INSERT INTO @tblChiNhanh
	select Name from splitstring(@ID_ChiNhanh);

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
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

		SELECT 
    	a.TenHangHoa,
		a.TenHangHoaFull,
		a.MaHangHoa,
		a.TenDonViTinh,
		a.ThuocTinh_GiaTri,
		a.TenLoHang,
		a.SoLuongBan,
		a.ThanhTien,
		a.SoLuongTra,
		a.GiaTriTra,
		a.GiamGiaHD,
		a.GiamTruThanhToanBaoHiem,
		a.DoanhThuThuan,
		iif(@XemGiaVon='1', a.TienVon,0) as TienVon,
		a.TienThue,
		a.ChiPhi,
		iif(@XemGiaVon='1',a.LaiLo,	0) as LaiLo	,
		iif(@XemGiaVon='1',ROUND(IIF(a.DoanhThuThuan = 0 OR a.DoanhThuThuan is null, 0,		
			a.LaiLo/ abs(a.DoanhThuThuan) * 100), 1),0) AS TySuat
    	FROM
    	(

		select 			
			hh.TenHangHoa,
			qdChuan.MaHangHoa,
			qdChuan.TenDonViTinh,
			qdChuan.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			concat(hh.TenHangHoa, qdChuan.ThuocTinhGiaTri) as TenHangHoaFull,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			lo.MaLoHang as TenLoHang,
			tblQD.ThanhTienTruocCK,
			tblQD.TienChietKhau,
			tblQD.SoLuongMua as SoLuongBan,
			tblQD.GiaTriMua as ThanhTien,
			tblQD.SoLuongTra,
			tblQD.GiaTriTra,
			tblQD.GiamGiaHangMua - tblQD.GiamGiaHangTra as GiamGiaHD,
			tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHangMua + tblQD.GiamTruThanhToanBaoHiem - tblQD.GiamGiaHangTra) as DoanhThuThuan,
			tblQD.GiaVonHangMua - tblQD.GiaVonHangTra as  TienVon,
			tblQD.TongThueHangMua - tblQD.TongThueHangTra as  TienThue,
			tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHangMua + tblQD.GiamTruThanhToanBaoHiem - tblQD.GiamGiaHangTra) - (tblQD.GiaVonHangMua - tblQD.GiaVonHangTra) - tblQD.ChiPhi  as  LaiLo,
			tblQD.GiamGiaHangMua,
			tblQD.GiamTruThanhToanBaoHiem,
			tblQD.GiamGiaHangTra,
			tblQD.TongThueHangMua,
			tblQD.TongThueHangTra,
			tblQD.ChiPhi
		from
		(
			select 
			qd.ID_HangHoa,
			tblMuaTra.ID_LoHang,
			sum(tblMuaTra.ThanhTienTruocCK) as ThanhTienTruocCK,
			sum(tblMuaTra.TienChietKhau) as TienChietKhau,
			sum(tblMuaTra.SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
			sum(tblMuaTra.GiaTriMua) as GiaTriMua,
			sum(tblMuaTra.TongThueHangMua) as TongThueHangMua,
			sum(tblMuaTra.GiamGiaHangMua) as GiamGiaHangMua,
			sum(tblMuaTra.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(tblMuaTra.GiaVonHangMua) as GiaVonHangMua,
			sum(tblMuaTra.SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
			sum(tblMuaTra.GiaTriTra) as GiaTriTra,
			sum(tblMuaTra.TongThueHangTra) as TongThueHangTra,
			sum(tblMuaTra.GiamGiaHangTra) as GiamGiaHangTra,
			sum(tblMuaTra.GiaVonHangTra) as GiaVonHangTra,
			sum(ISNULL(tblMuaTra.ChiPhi,0)) as ChiPhi			
		from
			(			
			select 
				ct.ID_DonViQuiDoi, ct.ID_LoHang,
				sum(SoLuong) as SoLuongMua,
				sum(ct.SoLuong * ct.DonGia) as ThanhTienTruocCK,
				sum(ct.SoLuong * ct.TienChietKhau) as TienChietKhau,
				sum(ThanhTien) as GiaTriMua,
				sum(ct.TienThue) as TongThueHangMua,
				sum(ct.GiamGiaHD) as GiamGiaHangMua,
				sum(ct.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
				sum(ct.TienVon) as GiaVonHangMua,				
				0 as SoLuongTra,
				0 as GiaTriTra,
				0 as TongThueHangTra,
				0 as GiamGiaHangTra,
				0 as GiaVonHangTra,
				max(ChiPhi) as ChiPhi
			from @tblCTHD ct		
			left join 
				(select ID_DonViQuiDoi, sum(ChiPhi) as ChiPhi from @tblChiPhi  group by ID_DonViQuiDoi
				 ) cp on ct.ID_DonViQuiDoi = cp.ID_DonViQuiDoi
			where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
			and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)	
			group by ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang

				---- giatritra + giavon hangtra

			union all
			select 
				ct.ID_DonViQuiDoi, ct.ID_LoHang,
				0 as SoLuongMua,
				0 as ThanhTienTruocCK,
				0 as TienChietKhau,
				0 as GiaTriMua,
				0 as TongThueHangMua,
				0 as GiamGiaHangMua,
				0 as GiamTruThanhToanBaoHiem,
				0 as GiaVonHangMua,
				sum(SoLuong) as SoLuongTra,
				sum(ThanhTien) as GiaTriTra,
				sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
				sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
				sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra,
				0 as ChiPhi
			from BH_HoaDon hd
			join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon 
			where hd.ChoThanhToan= 0
			and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
			and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
			and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
			and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID)
			and hd.LoaiHoaDon =6
			and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
			group by hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang
		) tblMuaTra 	
		join DonViQuiDoi qd on tblMuaTra.ID_DonViQuiDoi= qd.ID		
		group by qd.ID_HangHoa, tblMuaTra.ID_LoHang
	)tblQD
	join DM_HangHoa hh on tblQD.ID_HangHoa = hh.ID
	join DonViQuiDoi qdChuan on hh.ID= qdChuan.ID_HangHoa and qdChuan.LaDonViChuan=1
	left join DM_LoHang lo on hh.ID= lo.ID_HangHoa and tblQD.ID_LoHang = lo.ID
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)		
    	and hh.TheoDoi like @TheoDoi
		and qdChuan.Xoa like @TrangThai				
		AND ((select count(Name) from @tblSearchString b where 
				hh.TenHangHoa like '%'+b.Name+'%' 
    			OR hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    			or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    				or qdChuan.MaHangHoa like '%'+b.Name+'%'
    				or qdChuan.TenDonViTinh like '%' +b.Name +'%' 
					or lo.MaLoHang like '%' +b.Name +'%'
    			or qdChuan.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
		) a
		where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))
		order by a.MaHangHoa  	
  
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_NhomHang]
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
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu
	
		
		select 
			dtNhom.ID_NhomHang, dtNhom.TenNhomHangHoa,
			sum(SoLuong) as SoLuong,
			sum(TienChietKhau) as TienChietKhau,
			sum(ThanhTienTruocCK) as ThanhTienTruocCK,
			sum(ThanhTien) as ThanhTien,
			sum(GiamGiaHD) as GiamGiaHD,
			sum(GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(TienThue) as TienThue,
			sum(DoanhThu) as DoanhThu,
			iif(@XemGiaVon='1',sum(TienVon),0) as TienVon,
			iif(@XemGiaVon='1',sum(LaiLo)- sum(ChiPhi),0) as LaiLo,
			sum(ChiPhi) as ChiPhi
		from
		(
		select *
		from
		(
			select 
				hh.ID_NhomHang,		
				iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
				ISNULL(nhh.TenNhomHangHoa,  N'Nhóm hàng hóa mặc định') as TenNhomHangHoa,		
				cast(c.SoLuong as float) as SoLuong,		
				cast(c.ThanhTien as float) as ThanhTien,
				cast(c.GiamGiaHD as float) as GiamGiaHD,
				cast(c.GiamTruThanhToanBaoHiem as float) as GiamTruThanhToanBaoHiem,
				cast(c.TienThue as float) as TienThue,	
				cast(c.TienChietKhau as float) as TienChietKhau,
				cast(c.ThanhTienTruocCK as float) as ThanhTienTruocCK,
				iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
				cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem as float) as DoanhThu,
				iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.GiamTruThanhToanBaoHiem - c.TienVon as float),0) as LaiLo		,
				isnull(cp.ChiPhi,0) as ChiPhi
			from 
			(
			select 			
				sum(b.SoLuong * isnull(qd.TyLeChuyenDoi,1)) as SoLuong,
				sum(b.ThanhTien) as ThanhTien,
				sum(b.TienVon) as TienVon,
				qd.ID_HangHoa,
				b.ID_LoHang,			
				sum(b.GiamGiaHD) as GiamGiaHD,
				sum(b.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
				sum(b.TienThue) as TienThue,
				sum(b.TienChietKhau) as TienChietKhau,
				sum(b.ThanhTienTruocCK) as ThanhTienTruocCK
			from (
			select 					
				a.ID_LoHang, a.ID_DonViQuiDoi,									
				sum(isnull(a.TienThue,0)) as TienThue,
				sum(isnull(a.GiamGiaHD,0)) as GiamGiaHD,
				sum(isnull(a.GiamTruThanhToanBaoHiem,0)) as GiamTruThanhToanBaoHiem,
				sum(SoLuong) as SoLuong,
				sum(ThanhTien) as ThanhTien,
				sum(TienVon) as TienVon,
				sum(TienChietKhau) as TienChietKhau,
				sum(ThanhTienTruocCK) as ThanhTienTruocCK	
			from
			(			
				select 
					tblN.ID_DonViQuiDoi, tblN.ID_LoHang,
					sum(tblN.TienThue) as TienThue,
					sum(tblN.GiamGiaHD) as GiamGiaHD,
					sum(tblN.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
					sum(tblN.SoLuong) as SoLuong,
					sum(tblN.ThanhTien) as ThanhTien,
					sum(tblN.TienVon) as TienVon,
					sum(tblN.TienChietKhau) as TienChietKhau,
					sum(tblN.ThanhTienTruocCK) as ThanhTienTruocCK
				from
				(
					select 
							ct.ID,ct.ID_DonViQuiDoi, ct.ID_LoHang,
							ct.TienThue,
    						CT.GiamGiaHD,
							ct.GiamTruThanhToanBaoHiem,
							ct.SoLuong,
							ct.SoLuong * ct.TienChietKhau as TienChietKhau,
							ct.SoLuong * ct.DonGia as ThanhTienTruocCK,
							ct.ThanhTien, 	
							CT.TienVon
					from @tblCTHD ct					
					where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)						
					and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)						
					) tblN group by tblN.ID_LoHang, tblN.ID_DonViQuiDoi	
				) a group by a.ID_LoHang, a.ID_DonViQuiDoi
			)b
			join DonViQuiDoi qd on b.ID_DonViQuiDoi= qd.ID
			group by qd.ID_HangHoa, b.ID_LoHang
				) c
			join DM_HangHoa hh on c.ID_HangHoa = hh.ID
			join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_LoHang lo on c.ID_LoHang = lo.ID
			left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
			left join (
				select ID_DonViQuiDoi, sum(ChiPhi) as ChiPhi from @tblChiPhi group by ID_DonViQuiDoi
				) cp on qd.ID = cp.ID_DonViQuiDoi

			where 
			exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)	
    		and hh.TheoDoi like @TheoDoi
			and qd.Xoa like @TrangThai		
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
		) a where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))	
	)dtNhom
	group by dtNhom.ID_NhomHang, dtNhom.TenNhomHangHoa
	
  
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_TheoKhachHang]
    @SearchString NVARCHAR(MAX),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang uniqueidentifier,
    @ID_NhomKhachHang [nvarchar](max),
	@LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier]
AS
BEGIN	

	declare @tblChiNhanh table(ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh 
	select * from dbo.splitstring(@ID_ChiNhanh)
	
	DECLARE @LoadNhomMacDinh BIT = 0;
	IF(CHARINDEX('00000000-0000-0000-0000-000000000000', @ID_NhomKhachHang) > 0)
	BEGIN
		SET @LoadNhomMacDinh = 1;
	END
	
	DECLARE @tblIDNhoms TABLE (ID NVARCHAR(MAX))
	INSERT INTO @tblIDNhoms
	SELECT Name FROM splitstring(@ID_NhomKhachHang)

	DECLARE @tblIDNhomHang TABLE (ID NVARCHAR(MAX))
	INSERT INTO @tblIDNhomHang
	SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)

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
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu
	
	DECLARE @XemGiaVon as nvarchar
    Set @XemGiaVon = (Select 
    	Case when nd.LaAdmin = '1' then '1' else
    	Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    	From
    	HT_NguoiDung nd	
    	where nd.ID = @ID_NguoiDung);

		DECLARE @tblSearchString TABLE (Name [nvarchar](max));
		DECLARE @count int;
		INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@SearchString, ' ') where Name!='';
		Select @count =  (Select count(*) from @tblSearchString);

	
	select 	distinct
	tblV.ID_DoiTuong as ID_KhachHang, 
	tblV.TenNhomDoiTuongs as NhomKhachHang,
	tblV.MaDoiTuong as  MaKhachHang,
	tblV.TenDoiTuong as TenKhachHang,
	tblV.DienThoai, 
	round(tblV.SoLuongMua,3) as SoLuongMua,
	round(tblV.SoLuongTra,3) as SoLuongTra,
	round(tblV.GiaTriMua,3) as GiaTriMua,
	round(tblV.GiaTriTra,3) as GiaTriTra,
	round(tblV.SoLuong,3) as SoLuong,
	round(tblV.ThanhTien,3) as ThanhTien,
	round(iif(@XemGiaVon='1',tblV.TienVon,0),3) as TienVon,
	round(tblV.ThanhTien - tblV.GiamGiaHD,3) as DoanhThu,
	round(tblV.GiamGiaHD,3) as GiamGiaHD, 
	tblV.TongTienThue,
	round(iif(@XemGiaVon='1',tblV.ThanhTien - tblV.TienVon - tblV.GiamGiaHD - tblV.ChiPhi,0),3) as LaiLo,
	tblV.NguoiGioiThieu, tblV.NguoiQuanLy,
	ISNULL(tblV.TenNguonKhach,'') as TenNguonKhach,
	 tblV.ChiPhi
from(
select 
	tbldt.*,
	isnull(nvql.TenNhanVien,'') AS NguoiQuanLy,
	ISNULL(nvql.MaNhanVien,'') as MaNVQuanLy,
	isnull(dtgt.TenDoiTuong,'') AS NguoiGioiThieu,
	isnull(dtgt.MaDoiTuong,'') AS MaNguoiGioiThieu,
	isnull(dtgt.TenDoiTuong_KhongDau,'') AS NguoiGioiThieu_KhongDau,
	isnull(nk.TenNguonKhach,'') AS TenNguonKhach,
	isnull(cp.ChiPhi,0) as ChiPhi	
from 
(
Select 
	tbl.ID_DoiTuong,
	dt.MaDoiTuong,
	dt.TenDoiTuong,
	dt.TenDoiTuong_KhongDau,
	dt.TenDoiTuong_ChuCaiDau,
	dt.DienThoai,
	dt.TenNhomDoiTuongs,
	dt.ID_NguoiGioiThieu,
	dt.ID_NhanVienPhuTrach,
	dt.ID_NguonKhach,
	GiamTruThanhToanBaoHiem,
	iif(dt.IDNhomDoiTuongs is null or dt.IDNhomDoiTuongs ='','00000000-0000-0000-0000-000000000000',dt.IDNhomDoiTuongs)  as IDNhomDoiTuongs,
	isnull(tbl.SoLuongMua,0) as SoLuongMua,
	isnull(tbl.SoLuongTra,0) as SoLuongTra,
	isnull(tbl.SoLuongMua,0) - isnull(tbl.SoLuongTra,0) as SoLuong,
	isnull(tbl.GiaTriTra,0) - isnull(GiamGiaHangTra,0) as GiaTriTra,
	isnull(tbl.GiaTriMua,0) - isnull(GiamGiaHangMua,0) as GiaTriMua,
	isnull(tbl.GiaTriMua,0) - isnull(tbl.GiaTriTra,0) as ThanhTien,
	isnull(tbl.GiamGiaHangMua,0) - isnull(tbl.GiamGiaHangTra,0) as GiamGiaHD,	
	isnull(tbl.TongThueHangMua,0) - isnull(tbl.TongThueHangTra ,0) as TongTienThue,
	isnull(tbl.GiaVonHangMua,0) - isnull(tbl.GiaVonHangTra,0) as TienVon
	

from (

	select 
		tblMuaTra.ID_DoiTuong, 
		sum(SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
		sum(GiaTriMua) as GiaTriMua,
		sum(TongThueHangMua) as TongThueHangMua,
		sum(GiamGiaHangMua) as GiamGiaHangMua,
		sum(GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
		sum(GiaVonHangMua) as GiaVonHangMua,
		sum(SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
		sum(GiaTriTra) as GiaTriTra,
		sum(TongThueHangTra) as TongThueHangTra,
		sum(GiamGiaHangTra) as GiamGiaHangTra,
		sum(GiaVonHangTra) as GiaVonHangTra
	from
		(
		select 
				tbl.ID_DoiTuong,tbl.ID_DonViQuiDoi, tbl.ID_LoHang,
				sum(tbl.SoLuong) as SoLuongMua,
				sum(tbl.ThanhTien) as GiaTriMua,
				sum(tbl.TienThue) as TongThueHangMua,
				sum(tbl.GiamGiaHD) as GiamGiaHangMua,
				sum(tbl.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
				sum(tbl.TienVon) as GiaVonHangMua,
				0 as SoLuongTra,
				0 as GiaTriTra,
				0 as TongThueHangTra,
				0 as GiamGiaHangTra,
				0 as GiaVonHangTra
		from
		(
		---- giatrimua + giavon hangmua
			select 
				ct.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang,
				ct.SoLuong,
				ct.ThanhTien,
				ct.TienThue,
				ct.GiamGiaHD,
				ct.GiamTruThanhToanBaoHiem,
				ct.TienVon
			from @tblCTHD CT			
			where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)		
			and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)			
			) tbl
			group by tbl.ID_DoiTuong,tbl.ID_DonViQuiDoi, tbl.ID_LoHang

			---- giatritra + giavon hangtra
		union all
		select 
			hd.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			0 as SoLuongMua,
			0 as GiaTriMua,
			0 as TongThueHangMua,
			0 as GiamGiaHangMua,
			0 as GiamTruThanhToanBaoHiem,
			0 as GiaVonHangMua,
			sum(SoLuong) as SoLuongTra,
			sum(ThanhTien) as GiaTriTra,
			sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
			sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
			sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra
		from BH_HoaDon hd
		join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon 
		where hd.ChoThanhToan= 0
		and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo is null or ct.ID_ParentCombo= ct.ID)
		and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
		and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
		and hd.LoaiHoaDon =6
		and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
		group by hd.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang
	) tblMuaTra 
	join DonViQuiDoi qd on tblMuaTra.ID_DonViQuiDoi= qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	where 
	exists (SELECT ID FROM @tblIDNhomHang nhomhh where hh.ID_NhomHang= nhomhh.ID)
	and hh.TheoDoi like @TheoDoi
	and qd.Xoa like @TrangThai
	and iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) in (select name from dbo.splitstring(@LoaiHangHoa))
	group by tblMuaTra.ID_DoiTuong
) tbl 
join DM_DoiTuong dt on tbl.ID_DoiTuong= dt.ID
) tbldt
LEFT JOIN NS_NhanVien nvql ON nvql.ID = tbldt.ID_NhanVienPhuTrach
LEFT JOIN DM_DoiTuong dtgt ON dtgt.ID = tbldt.ID_NguoiGioiThieu
left join DM_NguonKhachHang nk on tbldt.ID_NguonKhach = nk.ID
left join (
		select ID_DoiTuong, sum(ChiPhi) as ChiPhi from @tblChiPhi group by ID_DoiTuong
		) cp on tbldt.ID_DoiTuong = cp.ID_DoiTuong
where 
 exists (select nhom1.Name 
			from dbo.splitstring(tbldt.IDNhomDoiTuongs) nhom1 
		 join @tblIDNhoms nhom2 on nhom1.Name = nhom2.ID) 
) tblV

where (select count(Name) from @tblSearchString b where 
				tblV.MaDoiTuong like '%'+b.Name+'%' 
    			OR tblV.TenDoiTuong like '%'+b.Name+'%' 
    			or tblV.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%' 
    			or tblV.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    			or tblV.TenNhomDoiTuongs like '%' +b.Name +'%' 
				or tblV.DienThoai like '%' +b.Name +'%'
    			or tblV.TenDoiTuong like '%'+b.Name+'%'
				or tblV.MaDoiTuong like '%'+b.Name+'%'
    				or tblV.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    				or tblV.TenDoiTuong_KhongDau like '%'+b.Name+'%'
					or tblV.MaNVQuanLy like '%'+b.Name+'%'
					or tblV.NguoiQuanLy like '%'+b.Name+'%'
					or tblV.MaNguoiGioiThieu like '%'+b.Name+'%'
					or tblV.NguoiGioiThieu like '%'+b.Name+'%'
					or tblV.NguoiGioiThieu_KhongDau like '%'+b.Name+'%'
    				)=@count or @count=0		


    END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHang_TheoNhanVien]
    @TenNhanVien [nvarchar](max),
    @timeStart [datetime],
    @timeEnd [datetime],
    @ID_ChiNhanh [nvarchar](max),
    @LoaiHangHoa [nvarchar](max),
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NhomHang uniqueidentifier,
	@LoaiChungTu [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@IDPhongBan nvarchar(max)
AS
BEGIN
    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    	Case when nd.LaAdmin = '1' then '1' else
    	Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    	From HT_NguoiDung nd	
    	where nd.ID = @ID_NguoiDung)

		DECLARE @tblChiNhanh TABLE (ID_DonVi uniqueidentifier)
		insert into @tblChiNhanh
		select * from splitstring(@ID_ChiNhanh)

		DECLARE @tblNhomHang TABLE (ID uniqueidentifier)
		insert into @tblNhomHang
		SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)

		DECLARE @tblLoaiChungTu TABLE(ID INT)
		INSERT INTO @tblLoaiChungTu
		select Name from splitstring(@LoaiChungTu);


		DECLARE @tblDepartment TABLE (ID_PhongBan uniqueidentifier)
		if @IDPhongBan =''
			insert into @tblDepartment
			select distinct ID_PhongBan from NS_QuaTrinhCongTac pb
		else
			insert into @tblDepartment
			select * from splitstring(@IDPhongBan)
	
	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
	DECLARE @count int;
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TenNhanVien, ' ') where Name!='';
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
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

    SELECT 
    	a.ID_NhanVien,
		a.MaNhanVien,
    	a.TenNhanVien, 
		a.SoLuongMua as SoLuongBan,
		cast(a.SoLuongTra as float) as SoLuongTra,
		a.GiaTriMua as ThanhTien,
		cast(a.GiaTriTra as float) as GiaTriTra,
		a.GiamGiaHangMua - a.GiamGiaHangTra as GiamGiaHD,
		a.GiamTruThanhToanBaoHiem,
		a.TongThueHangMua - a.TongThueHangTra as TienThue,
		isnull(cpOut.ChiPhi,0) as ChiPhi,
		IIF(@XemGiaVon = '1',a.GiaVonHangMua - a.GiaVonHangTra,0) as TienVon,
		a.GiaTriMua - a.GiaTriTra  - (a.GiamGiaHangMua + a.GiamTruThanhToanBaoHiem - a.GiamGiaHangTra) as DoanhThu,
		IIF(@XemGiaVon = '1',a.GiaTriMua - a.GiaTriTra  - (a.GiamGiaHangMua - a.GiamTruThanhToanBaoHiem - a.GiamGiaHangTra) - (a.GiaVonHangMua - a.GiaVonHangTra)-isnull(cpOut.ChiPhi,0),0) as LaiLo		
    	FROM
    	(

		select 
		tblMuaTra.ID_NhanVien, 
		nv.TenNhanVien, nv.MaNhanVien,
		sum(SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
		sum(GiaTriMua) as GiaTriMua,
		sum(TongThueHangMua) as TongThueHangMua,
		sum(GiamGiaHangMua) as GiamGiaHangMua,
		sum(GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
		sum(GiaVonHangMua) as GiaVonHangMua,
		sum(SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
		sum(GiaTriTra) as GiaTriTra,
		sum(TongThueHangTra) as TongThueHangTra,
		sum(GiamGiaHangTra) as GiamGiaHangTra,
		sum(GiaVonHangTra) as GiaVonHangTra
	from
		(		
		---- doanhthu + giavon hd le
		
		select 
			ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,			
			sum(SoLuong) as SoLuongMua,
			sum(ThanhTien) as GiaTriMua,
			sum(ct.TienThue) as TongThueHangMua,
			sum(ct.GiamGiaHD) as GiamGiaHangMua,
			sum(ct.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(ct.TienVon)	as GiaVonHangMua,			
			0 as SoLuongTra,
			0 as GiaTriTra,
			0 as TongThueHangTra,
			0 as GiamGiaHangTra,
			0 as GiaVonHangTra
		from @tblCTHD ct			
		where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)		
		group by ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang

			---- giatritra + giavon hangtra

		union all
		select 
			hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			0 as SoLuongMua,
			0 as GiaTriMua,
			0 as TongThueHangMua,
			0 as GiamGiaHangMua,
			0 as GiamTruThanhToanBaoHiem,
			0 as GiaVonHangMua,
			sum(SoLuong) as SoLuongTra,
			sum(ThanhTien) as GiaTriTra,
			sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
			sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
			sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra
		from BH_HoaDon hd		
		join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon
		where hd.ChoThanhToan= 0
		and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
		and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
		and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
		and exists (select Name from dbo.splitstring(@LoaiChungTu) ctu where hd.LoaiHoaDon= ctu.Name)
		and hd.LoaiHoaDon =6
		and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
		group by hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang
	) tblMuaTra 
	join NS_NhanVien nv on tblMuaTra.ID_NhanVien= nv.ID
	join DonViQuiDoi qd on tblMuaTra.ID_DonViQuiDoi= qd.ID
	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)
		and iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) in (select name from dbo.splitstring(@LoaiHangHoa))		
    	and hh.TheoDoi like @TheoDoi
		and qd.Xoa like @TrangThai		
		and exists (select pb.ID_PhongBan
				from @tblDepartment pb 
				join NS_QuaTrinhCongTac ct on pb.ID_PhongBan = ct.ID_PhongBan or  ct.ID_PhongBan is null
				where ct.ID_NhanVien = nv.ID )
		AND
		((select count(Name) from @tblSearchString b where 
			nv.TenNhanVien like '%'+b.Name+'%' 
    			or nv.TenNhanVienKhongDau like '%'+b.Name+'%' 
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%' +b.Name +'%' 
					or nv.DienThoaiDiDong like '%' +b.Name +'%'
					or nv.DienThoaiNhaRieng like '%' +b.Name +'%'
    		)=@count or @count=0)
		group by tblMuaTra.ID_NhanVien, nv.TenNhanVien, nv.MaNhanVien
    	) a
		left join (
			select cp.ID_NhanVien, sum(cp.ChiPhi) as Chiphi
			from @tblChiPhi cp
			group by cp.ID_NhanVien
		) cpOut on a.ID_NhanVien= cpOut.ID_NhanVien
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHangChiTiet_TheoKhachHang]
    @ID_KhachHang [uniqueidentifier],
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
    	where nd.ID = @ID_NguoiDung);

	declare @tblChiNhanh table(ID_DonVi uniqueidentifier)
	insert into @tblChiNhanh 
	select * from dbo.splitstring(@ID_ChiNhanh)

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
	exec BCBanHang_GetCTHD @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

	declare @tblChiPhi table (ID_ParentCombo uniqueidentifier,ID_DonViQuiDoi uniqueidentifier, ChiPhi float, 
		ID_NhanVien uniqueidentifier,ID_DoiTuong uniqueidentifier)
	insert into @tblChiPhi
	exec BCBanHang_GetChiPhi @ID_ChiNhanh, @timeStart, @timeEnd, @LoaiChungTu

		select *
		from
		(
		select 					
			hh.TenHangHoa,
			qdChuan.MaHangHoa,
			qdChuan.TenDonViTinh,
			qdChuan.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			concat(hh.TenHangHoa, qdChuan.ThuocTinhGiaTri) as TenHangHoaFull,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			lo.MaLoHang as TenLoHang,
			tblQD.ChiPhi,
			tblQD.SoLuongMua,
			tblQD.GiaTriMua,
			tblQD.SoLuongTra,
			tblQD.GiaTriTra,
			tblQD.SoLuongMua - tblQD.SoLuongTra as SoLuong,
			tblQD.GiaTriMua - tblQD.GiaTriTra as ThanhTien,
			tblQD.GiamGiaHDMua - tblQD.GiamGiaHDTra as GiamGiaHD,
			tblQD.GiamTruThanhToanBaoHiem,
			tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHDMua + tblQD.GiamTruThanhToanBaoHiem - tblQD.GiamGiaHDTra) as DoanhThuThuan,		
			iif(@XemGiaVon = '1',tblQD.GVMua - tblQD.GVTra,1) as  TienVon,
			tblQD.ThueHDMua - tblQD.ThueHDTra as  TongTienThue,
			iif(@XemGiaVon = '1',tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHDMua + tblQD.GiamTruThanhToanBaoHiem - tblQD.GiamGiaHDTra) 
			- (tblQD.GVMua - tblQD.GVTra) -tblQD.ChiPhi,1) as  LaiLo			
		from
		(
	SELECT 	qd.ID_HangHoa, tbl.ID_LoHang,			
		sum(SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
		sum(SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
		sum(tbl.GiaTriMua - ISNULL(tbl.GiamGiaHangMua,0)) as GiaTriMua,
		sum(isnull(tbl.GiaTriTra,0) - isnull(GiamGiaHangTra,0)) as GiaTriTra ,
		sum(tbl.TongThueHangMua) as ThueHDMua,
		sum(isnull(tbl.TongThueHangTra,0)) as ThueHDTra ,
		sum(tbl.GiamGiaHangMua) as GiamGiaHDMua,
		sum(tbl.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
		sum(isnull(tbl.GiamGiaHangTra,0)) as GiamGiaHDTra,	
		sum(tbl.GiaVonHangMua) as GVMua,
		sum(isnull(tbl.GiaVonHangTra,0)) as GVTra,
		sum(isnull(cp.ChiPhi,0)) as ChiPhi	
FROM
    (
		select 
			ct.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			sum(SoLuong) as SoLuongMua,
			sum(ThanhTien) as GiaTriMua,
			sum(ct.TienThue) as TongThueHangMua,
			sum(ct.GiamGiaHD) as GiamGiaHangMua,
			sum(ct.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(ct.TienVon) as GiaVonHangMua,		
			0 as SoLuongTra,
			0 as GiaTriTra,
			0 as TongThueHangTra,
			0 as GiamGiaHangTra,
			0 as GiaVonHangTra
		from @tblCTHD ct		
		where ct.ID_DoiTuong= @ID_KhachHang
		and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)		
		group by ct.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang

			---- giatritra + giavon hangtra

		union all
		select 
			hd.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang,
			0 as SoLuongMua,
			0 as GiaTriMua,
			0 as TongThueHangMua,
			0 as GiamGiaHangMua,
			0 as GiamTruThanhToanBaoHiem,
			0 as GiaVonHangMua,
			sum(SoLuong) as SoLuongTra,
			sum(ThanhTien) as GiaTriTra,
			sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
			sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
			sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra
		from BH_HoaDon hd
		join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon 
		where hd.ChoThanhToan= 0
		and hd.ID_DoiTuong= @ID_KhachHang
		and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
		and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
		and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
		and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
		and hd.LoaiHoaDon =6
		and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
		group by hd.ID_DoiTuong,ct.ID_DonViQuiDoi, ct.ID_LoHang
		)tbl	
		join DonViQuiDoi qd on tbl.ID_DonViQuiDoi= qd.ID
		left join 
			(select ID_DonViQuiDoi, sum(ChiPhi) as ChiPhi from @tblChiPhi where ID_DoiTuong= @ID_KhachHang group by ID_DonViQuiDoi
			 ) cp on qd.ID = cp.ID_DonViQuiDoi
		group by qd.ID_HangHoa, tbl.ID_LoHang
	) tblQD
	join DM_HangHoa hh on tblQD.ID_HangHoa = hh.ID
	join DonViQuiDoi qdChuan on hh.ID= qdChuan.ID_HangHoa and qdChuan.LaDonViChuan=1
	left join DM_LoHang lo on hh.ID= lo.ID_HangHoa and tblQD.ID_LoHang = lo.ID
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
		where 
		exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)		
    	and hh.TheoDoi like @TheoDoi
		and qdChuan.Xoa like @TrangThai				
	) a where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))	
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoBanHangChiTiet_TheoNhanVien]
    @ID_NhanVien UNIQUEIDENTIFIER,
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

		declare @tblChiNhanh table(ID_DonVi uniqueidentifier)
		insert into @tblChiNhanh
		select Name from dbo.splitstring(@ID_ChiNhanh)

		declare @tblNhomHang table(ID_NhomHang uniqueidentifier)
		insert into @tblNhomHang
		SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang)


    DECLARE @XemGiaVon as nvarchar
    	Set @XemGiaVon = (Select 
    	Case when nd.LaAdmin = '1' then '1' else
    	Case when nd.XemGiaVon is null then '0' else nd.XemGiaVon end end as XemGiaVon
    	From HT_NguoiDung nd	
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
		GiamTruThanhToanBaoHiem float,
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
	from
	(
    SELECT  
			hh.TenHangHoa,
			qdChuan.MaHangHoa,
			qdChuan.TenDonViTinh,
			qdChuan.ThuocTinhGiaTri as ThuocTinh_GiaTri,
			concat(hh.TenHangHoa, qdChuan.ThuocTinhGiaTri) as TenHangHoaFull,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa = '1', 1, 2), hh.LoaiHangHoa) as LoaiHangHoa,
			lo.MaLoHang as TenLoHang,
			tblQD.SoLuongMua as SoLuongBan,
			tblQD.GiaTriMua as ThanhTien,
			tblQD.SoLuongTra,
			tblQD.GiaTriTra,	
			isnull(cp.ChiPhi,0) as ChiPhi,
			tblQD.GiamGiaHDMua - tblQD.GiamGiaHDTra as GiamGiaHD,		
			tblQD.GiamTruThanhToanBaoHiem,
			iif(@XemGiaVon = '1',tblQD.GVMua - tblQD.GVTra,1) as  TienVon,
			tblQD.ThueHDMua - tblQD.ThueHDTra as  TongTienThue,
			iif(@XemGiaVon = '1',tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHDMua + tblQD.GiamTruThanhToanBaoHiem - tblQD.GiamGiaHDTra) - (tblQD.GVMua - tblQD.GVTra) -isnull(cp.ChiPhi,0),1) as  LaiLo
    	FROM
    	(
		SELECT 	qd.ID_HangHoa, tbl.ID_LoHang,			
			sum(SoLuongMua  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongMua,
			sum(SoLuongTra  * isnull(qd.TyLeChuyenDoi,1)) as SoLuongTra,
			sum(tbl.GiaTriMua) as GiaTriMua,
			sum(isnull(tbl.GiaTriTra,0)) as GiaTriTra ,
			sum(tbl.TongThueHangMua) as ThueHDMua,
			sum(isnull(tbl.TongThueHangTra,0)) as ThueHDTra ,
			sum(tbl.GiamGiaHangMua) as GiamGiaHDMua,
			sum(tbl.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
			sum(isnull(tbl.GiamGiaHangTra,0)) as GiamGiaHDTra,				
			sum(tbl.GiaVonHangMua) as GVMua,
			sum(isnull(tbl.GiaVonHangTra,0)) as GVTra	
		FROM
		(		
				select 
					ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,
					sum(SoLuong) as SoLuongMua,
					sum(ct.ThanhTien) as GiaTriMua,
					sum(ct.TienThue) as TongThueHangMua,
					sum(ct.GiamGiaHD) as GiamGiaHangMua,
					sum(ct.GiamTruThanhToanBaoHiem) as GiamTruThanhToanBaoHiem,
					sum(ct.TienVon) as GiaVonHangMua,					
					0 as SoLuongTra,
					0 as GiaTriTra,
					0 as TongThueHangTra,
					0 as GiamGiaHangTra,
					0 as GiaVonHangTra
				from @tblCTHD ct				
				where (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
				and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
				and ct.ID_NhanVien= @ID_NhanVien
				group by ct.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang

					---- giatritra + giavon hangtra

				union all
				select 
					hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang,
					0 as SoLuongMua,
					0 as GiaTriMua,
					0 as TongThueHangMua,
					0 as GiamGiaHangMua,
					0 as GiamTruThanhToanBaoHiem,
					0 as GiaVonHangMua,
					sum(SoLuong) as SoLuongTra,
					sum(ThanhTien) as GiaTriTra,
					sum(ct.TienThue * ct.SoLuong) as TienThueHangTra,
					sum(iif(hd.TongTienHang=0,0, ct.ThanhTien  * hd.TongGiamGia /hd.TongTienHang)) as GiamGiaHangTra,
					sum(ct.SoLuong * ct.GiaVon) as GiaVonHangTra
				from BH_HoaDon hd
				join BH_HoaDon_ChiTiet ct on hd.id= ct.ID_HoaDon 
				where hd.ChoThanhToan= 0
				and hd.ID_NhanVien= @ID_NhanVien
				and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)
				and (ct.ID_ParentCombo = ct.ID or ct.ID_ParentCombo is null)
				and hd.NgayLapHoaDon >= @timeStart and hd.NgayLapHoaDon < @timeEnd
				and exists (select ID_DonVi from @tblChiNhanh dv where hd.ID_DonVi= dv.ID_DonVi)
				and hd.LoaiHoaDon =6
				and (ct.ChatLieu is null or ct.ChatLieu !='4') ---- khong lay ct sudung dichvu
				group by hd.ID_NhanVien,ct.ID_DonViQuiDoi, ct.ID_LoHang
				)tbl	
				join DonViQuiDoi qd on tbl.ID_DonViQuiDoi= qd.ID
				group by qd.ID_HangHoa, tbl.ID_LoHang
   		
    	) tblQD
		join DM_HangHoa hh on tblQD.ID_HangHoa = hh.ID
	join DonViQuiDoi qdChuan on hh.ID= qdChuan.ID_HangHoa and qdChuan.LaDonViChuan=1
	left join DM_LoHang lo on hh.ID= lo.ID_HangHoa and tblQD.ID_LoHang = lo.ID
	left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
	left join (
		select ID_DonViQuiDoi, sum(ChiPhi) as ChiPhi from @tblChiPhi group by ID_DonViQuiDoi
		) cp on qdChuan.ID= cp.ID_DonViQuiDoi
	where 
	exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhh.ID= allnhh.ID)
    and hh.TheoDoi like @TheoDoi
	and qdChuan.Xoa like @TrangThai	
	) a where a.LoaiHangHoa in (select name from dbo.splitstring(@LoaiHangHoa))	
	order by a.SoLuongBan desc
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDatHang_NhomHang]
    @TenNhomHang [nvarchar](max),
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
	INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TenNhomHang, ' ') where Name!='';
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

	select 
		nhh.ID,
		sum(tblDH.SoLuongDat) as SoLuongDat,
		sum(tblDH.ThanhTien) as ThanhTien,
		sum(tblDH.GiamGiaHD) as GiamGiaHD,
		sum(tblDH.ThanhTien - tblDH.GiamGiaHD) as GiaTriDat,	
		sum(isnull(tblDH.SoLuongNhan,0)) as SoLuongNhan,
		isnull(nhh.TenNhomHangHoa,N'Nhóm mặc định') as TenNhomHangHoa
	from(
	select ct.ID_DonViQuiDoi, ct.ID_LoHang, 
		sum(ct.SoLuong) as SoLuongDat, 
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
		and iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) in (select LoaiHang from @tblLoai)
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
    				or qd.ThuocTinhGiaTri like '%'+b.Name+'%')=@count or @count=0)
	group by nhh.ID, nhh.TenNhomHangHoa   
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
		tblDH.ThanhTien - tblDH.GiamGiaHD as GiaTriDat,
		tblDH.SoLuongNhan,
		isnull(nhh.TenNhomHangHoa,N'Nhóm mặc định') as TenNhomHangHoa,
		iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa
	from(
	select ct.ID_DonViQuiDoi, ct.ID_LoHang, 
		sum(ct.SoLuong) as SoLuongDat, 
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

			Sql(@"ALTER PROCEDURE [dbo].[GetInforHoaDon_ByID]
    @ID_HoaDon [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
    
    declare @IDHDGoc uniqueidentifier, @ID_DoiTuong uniqueidentifier, @ID_BaoHiem uniqueidentifier
    select @IDHDGoc = ID_HoaDon,  @ID_DoiTuong = ID_DoiTuong, @ID_BaoHiem = ID_BaoHiem from BH_HoaDon where ID = @ID_HoaDon

   
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
				iif(hd.LoaiHoaDon=6 or hd.LoaiHoaDon = 4, isnull(hd.TongChiPhi,0) , isnull(hd.ChiPhi,0)) as TongChiPhi, --- loai = 6: PhiTraHag (khach phai tra)
    
    			CAST(ISNULL(TienDoiDiem,0) as float) as TienDoiDiem,	
    			CAST(ISNULL(ThuTuThe,0) as float) as ThuTuThe,	
    			isnull(soquy.TienMat,0) as TienMat,
    			isnull(soquy.TienATM,0) as TienATM,
    			isnull(soquy.ChuyenKhoan,0) as ChuyenKhoan,
				cast(isnull(soquy.BaoHiemDaTra,0) as float) as BaoHiemDaTra,
				cast(ISNULL(KhachDaTra,0)as float)  as KhachDaTra,

				cast(ISNULL(soquy.KhachDaTra,0) + isnull(soquy.BaoHiemDaTra,0) as float) as DaThanhToan,
    
    			dt.MaDoiTuong,
				dt.DienThoai,
				dt.Email,
				dt.DiaChi, 
				dt.MaSoThue,
				dt.TaiKhoanNganHang,

    			bh.TenDoiTuong as TenBaoHiem,
    			bh.MaDoiTuong as MaBaoHiem,
				isnull(bh.Email,'') as BH_Email,
    			isnull(bh.DiaChi,'') as BH_DiaChi,
    			isnull(bh.DienThoai,'') as DienThoaiBaoHiem,

				xe.BienSo,
    			tn.MaPhieuTiepNhan,
				tn.SoKmVao,
				tn.SoKmRa,

    			ISNULL(dt.TenDoiTuong,N'Khách lẻ')  as TenDoiTuong,
    			ISNULL(bg.TenGiaBan,N'Bảng giá chung') as TenBangGia,
    			ISNULL(nv.TenNhanVien,N'')  as TenNhanVien,
    			ISNULL(dv.TenDonVi,N'')  as TenDonVi,    

    			case when hd.NgayApDungGoiDV is null then '' else  convert(varchar(14), hd.NgayApDungGoiDV ,103) end  as NgayApDungGoiDV,
    			case when hd.HanSuDungGoiDV is null then '' else  convert(varchar(14), hd.HanSuDungGoiDV ,103) end as HanSuDungGoiDV,

				---- get 2 trường này chỉ mục đích KhuyenMai thooi daay !!!---
				case when dt.IDNhomDoiTuongs='' then '00000000-0000-0000-0000-000000000000' 
					else  ISNULL(dt.IDNhomDoiTuongs,'00000000-0000-0000-0000-000000000000') end as IDNhomDoiTuongs,
				dt.NgaySinh_NgayTLap, 

				iif(hd.ID_BaoHiem is null,'',tn.NguoiLienHeBH) as LienHeBaoHiem,
				iif(hd.ID_BaoHiem is null,'',tn.SoDienThoaiLienHeBH) as SoDienThoaiLienHeBaoHiem,

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

				cpVC.ID_NhaCungCap,
				iif(cpVC.ID_NhaCungCap = hd.ID_DoiTuong,0,isnull(cpVC.DaChi_BenVCKhac,0)) as DaChi_BenVCKhac, ---- nếu chính nó VC, đã chi VC = 0
				isnull(cpVC.TenDoiTuong,'') as TenNCCVanChuyen, 
    			isnull(cpVC.MaDoiTuong,'') as MaNCCVanChuyen, 
				ISNULL(baogia.MaHoaDon,'') as MaBaoGia,

    			
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
    	from BH_HoaDon hd
		left join Gara_PhieuTiepNhan tn on tn.ID= hd.ID_PhieuTiepNhan
		left join Gara_DanhMucXe xe on hd.ID_Xe= xe.ID
    	left join DM_DoiTuong dt on hd.ID_DoiTuong= dt.ID
    	left join NS_NhanVien nv on hd.ID_NhanVien = nv.ID
    	left join DM_DonVi dv on hd.ID_DonVi= dv.ID
    	left join DM_GiaBan bg on hd.ID_BangGia= bg.ID
    	left join DM_DoiTuong bh on hd.ID_BaoHiem= bh.ID
		left join BH_HoaDon baogia on hd.ID_HoaDon= bg.ID and baogia.ID_PhieuTiepNhan= tn.ID
		left join
		(
			select cp.ID_NhaCungCap,
				cp.ID_HoaDon,
				ncc.MaDoiTuong, 
				ncc.TenDoiTuong,
				isnull(TienThu,0)  as DaChi_BenVCKhac
			from BH_HoaDon_ChiPhi cp 
			join DM_DoiTuong ncc on cp.ID_NhaCungCap= ncc.ID
			left join
			(
				select 
					qct.ID_HoaDonLienQuan,   	
					qct.ID_DoiTuong,   
					sum(iif(qhd.LoaiHoaDon = 11,-qct.TienThu, qct.TienThu)) as TienThu
				from Quy_HoaDon qhd
				join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon= qhd.ID
				where qct.ID_HoaDonLienQuan = @ID_HoaDon
				and (qhd.TrangThai= 1 or qhd.TrangThai is null)				
				group by qct.ID_HoaDonLienQuan,  qct.ID_DoiTuong		
			) chiVC on chiVC.ID_DoiTuong = cp.ID_NhaCungCap 
			where cp.ID_HoaDon = @ID_HoaDon 
			and cp.ID_HoaDon_ChiTiet is null			
		) cpVC on hd.ID= cpVC.ID_HoaDon
    	left join 
    		(
    				select 
    					hdsq.ID,
    					sum(Khach_TienMat + BH_TienMat) as TienMat,
    					sum(Khach_TienPOS + BH_TienPOS) as TienATM,
    					sum(Khach_TienCK + BH_TienCK) as ChuyenKhoan,
    					sum(Khach_TienDiem) as TienDoiDiem,
    					sum(Khach_TheGiaTri) as ThuTuThe,		
						sum(Khach_TienCoc + BH_TienCoc) as TienDatCoc,		

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
								hdFist.ID,
								isnull(thuDH.Khach_TienMat,0) as Khach_TienMat,
								isnull(thuDH.Khach_TienPOS,0) as Khach_TienPOS,
								isnull(thuDH.Khach_TienCK,0) as Khach_TienCK,
								isnull(thuDH.Khach_TheGiaTri,0) as Khach_TheGiaTri,
								isnull(thuDH.Khach_TienDiem,0) as Khach_TienDiem,
								isnull(thuDH.Khach_TienCoc,0) as Khach_TienCoc,
								isnull(thuDH.TienThu,0) as KhachDaTra,
								isnull(thuDH.TienThu,0) as ThuDatHang,
								0 as BH_TienMat,
								0 as BH_TienPOS,
								0 as BH_TienCK,
								0 as BH_TheGiaTri,
								0 as BH_TienDiem,
								0 as BH_TienCoc,
								0 as BaoHiemDaTra
							from
							(
								------- 1. get list hoadon duoc xuly tu hdDatHang (neu loaiHD = 1,25) ----
							select hdXLy.Id,
								hdXLy.ID_HoaDon,
								hdXLy.NgayLapHoaDon,
								ROW_NUMBER() OVER(PARTITION BY hdXLy.ID_HoaDon ORDER BY hdXLy.NgayLapHoaDon ASC) AS isFirst	
							from BH_HoaDon hdXLy
							where hdXLy.ID_HoaDon = @IDHDGoc
							and hdXLy.ChoThanhToan='0'
							) hdFist
							left join
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
								where qct.ID_HoaDonLienQuan = @IDHDGoc
								and (qhd.TrangThai= 1 Or qhd.TrangThai is null)		
								group by qct.ID_HoaDonLienQuan

							)thuDH on thuDH.ID_DatHang= hdFist.ID_HoaDon
							where hdFist.isFirst = 1

						
							union all

								--- if hdDatHang: get thuChi all hdXuLy ---
							select 
								@ID_HoaDon as ID,
								sum(iif(qct.HinhThucThanhToan=1,iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TienMat,
								sum(iif(qct.HinhThucThanhToan=2, iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TienPOS,
								sum(iif(qct.HinhThucThanhToan=3, iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TienCK,
								sum(iif(qct.HinhThucThanhToan=4, iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TheGiaTri,
								sum(iif(qct.HinhThucThanhToan=5, iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TienDiem,
								sum(iif(qct.HinhThucThanhToan=6, iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), 0)) as Khach_TienCoc,		
								sum(iif(qhd.LoaiHoaDon = 11,iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), iif(qct.ID_DoiTuong = @ID_DoiTuong, -qct.TienThu,0))) as KhachDaTra,
								0 as ThuDatHang,

								sum(iif(qct.HinhThucThanhToan=1,iif(qct.ID_DoiTuong = @ID_BaoHiem, qct.TienThu,0), 0)) as BH_TienMat,
								sum(iif(qct.HinhThucThanhToan=2, iif(qct.ID_DoiTuong = @ID_BaoHiem, qct.TienThu,0), 0)) as BH_TienPOS,
								sum(iif(qct.HinhThucThanhToan=3, iif(qct.ID_DoiTuong = @ID_BaoHiem, qct.TienThu,0), 0)) as BH_TienCK,
								cast(0 as float) as BH_TheGiaTri,
								cast(0 as float) as BH_TienDiem,
								sum(iif(qct.HinhThucThanhToan=6, iif(qct.ID_DoiTuong = @ID_BaoHiem, qct.TienThu,0), 0)) as BH_TienCoc,		
								sum(iif(qhd.LoaiHoaDon = 11,iif(qct.ID_DoiTuong = @ID_DoiTuong, qct.TienThu,0), iif(qct.ID_DoiTuong = @ID_BaoHiem, -qct.TienThu,0))) as BaoHiemDaTra
							from Quy_HoaDon qhd
							join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDon = qhd.ID
							where (qhd.TrangThai= 1 Or qhd.TrangThai is null)		
							and exists (
								select hdXLy.Id,							
									hdXLy.NgayLapHoaDon
								from BH_HoaDon hdXLy
								where hdXLy.ID_HoaDon = @ID_HoaDon and qct.ID_HoaDonLienQuan = hdXLy.ID
								and hdXLy.ChoThanhToan='0'
								and hdXLy.LoaiHoaDon in (1,25)
							)
							group by qct.ID_DoiTuong
    
    						union all

    					---- khach datra
    					select qct.ID_HoaDonLienQuan,	
							sum(iif(qct.HinhThucThanhToan=1, qct.TienThu, 0)) as Khach_TienMat,
							sum(iif(qct.HinhThucThanhToan=2, qct.TienThu, 0)) as Khach_TienPOS,
							sum(iif(qct.HinhThucThanhToan=3, qct.TienThu, 0)) as Khach_TienCK,
							sum(iif(qct.HinhThucThanhToan=4, qct.TienThu, 0)) as Khach_TheGiaTri,
							sum(iif(qct.HinhThucThanhToan=5, qct.TienThu, 0)) as Khach_TienDiem,
							sum(iif(qct.HinhThucThanhToan=6, qct.TienThu, 0)) as Khach_TienCoc,	
							sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as KhachDaTra,

							0 as ThuDatHang,
							0 as BH_TienMat,
							0 as BH_TienPOS,
							0 as BH_TienCK,
							0 as BH_TheGiaTri,
							0 as BH_TienDiem,
							0 as BH_TienCoc,
							0 as BaoHiemDaTra

    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					where qhd.TrangThai= 1
    					and qct.ID_DoiTuong= @ID_DoiTuong and qct.ID_HoaDonLienQuan = @ID_HoaDon					
    					group by qct.ID_HoaDonLienQuan
    
    
    					union all
    					----- baohiem datra
    					select qct.ID_HoaDonLienQuan, 
    						0 as Khach_TienMat,
							0 as Khach_TienPOS,
							0 as Khach_TienCK,
							0 as Khach_TheGiaTri,
							0 as Khach_TienDiem,
							0 as Khach_TienCoc,
							0 as KhachDaTra, 
							0 as ThuDatHang,

							sum(iif(qct.HinhThucThanhToan=1, qct.TienThu, 0)) as BH_TienMat,
							sum(iif(qct.HinhThucThanhToan=2, qct.TienThu, 0)) as BH_TienPOS,
							sum(iif(qct.HinhThucThanhToan=3, qct.TienThu, 0)) as BH_TienCK,
							sum(iif(qct.HinhThucThanhToan=4, qct.TienThu, 0)) as BH_TheGiaTri,
							sum(iif(qct.HinhThucThanhToan=5, qct.TienThu, 0)) as BH_TienDiem,
							sum(iif(qct.HinhThucThanhToan=6, qct.TienThu, 0)) as BH_TienCoc,	
							sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as BaoHiemDaTra

    					from Quy_HoaDon_ChiTiet qct
    					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    					where qhd.TrangThai= 1 
    					and qct.ID_DoiTuong= @ID_BaoHiem and qct.ID_HoaDonLienQuan = @ID_HoaDon							
    					group by qct.ID_HoaDonLienQuan
    				)hdsq group by hdsq.ID
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
					iif(hd.LoaiHoaDon=4,0, ISNULL(allTra.TongGtriTra,0)) as TongGiaTriTra		
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
				when 3 then  hd.PhaiThanhToanBaoHiem
			else
			 ---- same NCC = ben VC
				iif(cp.ID_NhaCungCap = hd.ID_Doituong, hd.PhaiThanhToan, 
					--- if phieuchi nay la chi ben VC: phaiTT = chiphiVC
					--- else: tru chi phi VC
					iif(qct.ID_DoiTuong = cp.ID_NhaCungCap, cp.PhaiThanhToan, hd.PhaiThanhToan - isnull(cp.PhaiThanhToan,0) ))
				end as TongThanhToanHD,
					
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
	left join #tblChiPhi cp on hd.ID= cp.ID_HoaDon and qct.ID_HoaDonLienQuan = cp.ID_HoaDon and cp.ID_NhaCungCap = qct.ID_DoiTuong
    left join DM_DoiTuong dt on qct.ID_DoiTuong= dt.ID
	left join NS_NhanVien nv on qhd.ID_NhanVien= nv.ID
	left join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi = ktc.ID
	left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang = tk.ID
	left join DM_NganHang nh on tk.ID_NganHang = nh.ID
	left join #thuChinhNo thisSQ  on thisSQ.ID_HoaDonLienQuan = qct.ID_HoaDonLienQuan and thisSQ.ID_DoiTuong = qct.ID_DoiTuong
    where qhd.ID= @ID
	order by hd.NgayLapHoaDon
END");

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
				ID_Xe,
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
					hd.ID_Xe,
					ISNULL(hd.TongChiPhi,0) as TongChiPhi -- used check when thanhtoan nhaphang
				from
					(
					select hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
						hd.TongTienThue,
						hd.TongThanhToan,
    					ISNULL(hd.PhaiThanhToan,0)  as PhaiThanhToan,
						ISNULL(hd.PhaiThanhToanBaoHiem,0)  as PhaiThanhToanBaoHiem,
						hd.TongChiPhi,
						cpVC.ID_NhaCungCap,
						hd.ID_Xe
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

		select  tblView.*,
			xe.BienSo
		from
		(
			select 
					hd.ID, hd.MaHoaDon, hd.NgayLapHoaDon, hd.LoaiHoaDon,
					hd.TongThanhToan,
					hd.TongTienThue,
					TinhChietKhauTheo,
					iif(@LoaiDoiTuong=3,0,isnull(cn.BuTruTraHang, 0)) as TongTienHDTra,
					hd.PhaiThanhToan,
					hd.ID_Xe
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
				sum(cp.ThanhTien) as PhaiThanhToan,
				hd.ID_Xe
			from BH_HoaDon_ChiPhi cp
			join BH_HoaDon hd on cp.ID_HoaDon = hd.ID
			where hd.ChoThanhToan= 0
			and cp.ID_NhaCungCap= @ID_DoiTuong
			group by cp.ID_HoaDon, hd.MaHoaDon, hd.NgayLapHoaDon,	hd.LoaiHoaDon,hd.ID_Xe
		)tblView
		left join Gara_DanhMucXe xe on tblView.ID_Xe = xe.ID
		order by NgayLapHoaDon desc
END");

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
		c.MaBaoHiem, c.TenBaoHiem, c.BH_SDT,
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

			Sql(@"ALTER PROCEDURE [dbo].[ReportDiscountProduct_Detail]
    @ID_ChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [nvarchar](max),
    @ID_NhomHang [nvarchar](max),
	@LaHangHoas [nvarchar](max),
	@LoaiChungTus [nvarchar](max),
    @TextSearch [nvarchar](max),
    @TextSearchHangHoa [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status_ColumHide [int],
    @StatusInvoice [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    set nocount on;
    	set @DateTo = DATEADD(day,1,@DateTo)
    
		declare @tblLoaiHang table (LoaiHang int)
    	insert into @tblLoaiHang
    	select Name from dbo.splitstring(@LaHangHoas)

		declare @tblChungTu table (LoaiChungTu int)
    	insert into @tblChungTu
    	select Name from dbo.splitstring(@LoaiChungTus)

    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanhs,'BCCKHangHoa_XemDS_PhongBan','BCCKHangHoa_XemDS_HeThong');
    
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    DECLARE @count int =  (Select count(*) from @tblSearchString);
    
    
    	DECLARE @tblSearchHH TABLE (Name [nvarchar](max));	
    INSERT INTO @tblSearchHH(Name) select  Name from [dbo].[splitstringByChar](@TextSearchHangHoa, ' ') where Name!='';
    DECLARE @countHH int =  (Select count(*) from @tblSearchHH);
    
    	declare @tblIDNhom table (ID uniqueidentifier);
    	if @ID_NhomHang='%%' OR @ID_NhomHang =''
    		begin
    			insert into @tblIDNhom
    			select ID from DM_NhomHangHoa
    		end
    	else
    		begin
    			insert into @tblIDNhom
    			select cast(Name as uniqueidentifier) from dbo.splitstring(@ID_NhomHang)
    		end;
    
    		select 
				ID_HoaDon,
				ID_ChiTietHoaDon,
				ID_DonViQuiDoi,
				ID_LoHang,
				ID_NhanVien,
				MaHoaDon, 
				LoaiHoaDon,
    			NgayLapHoaDon,
    			MaHangHoa,
    			MaNhanVien,
    			TenNhanVien,
    			TenNhomHangHoa,
    			ID_NhomHang,
    			TenHangHoa,
    			TenHangHoaFull,
    			TenDonViTinh,
    			TenLoHang,
    			ThuocTinh_GiaTri,
    			HoaHongThucHien,
    			PTThucHien,
    			HoaHongTuVan,
    			PTTuVan,
    			HoaHongBanGoiDV,
    			PTBanGoi,
    			HoaHongThucHien_TheoYC,
    			PTThucHien_TheoYC,
    			SoLuong,
    			ThanhTien,
    			HeSo,
				ThanhTien * HeSo as GtriSauHeSo,
    			ISNULL(MaDoiTuong,'') as MaKhachHang,
    			ISNULL(TenDoiTuong,N'Khách lẻ') as TenKhachHang,
    			ISNULL(dt.DienThoai,'') as DienThoaiKH,		
    		case @Status_ColumHide
    					when  1 then cast(0 as float)
    					when  2 then ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  3 then ISNULL(HoaHongBanGoiDV,0.0)
    					when  4 then ISNULL(HoaHongBanGoiDV,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  5 then ISNULL(HoaHongTuVan,0.0)
    					when  6 then ISNULL(HoaHongThucHien_TheoYC,0.0) + ISNULL(HoaHongTuVan,0.0)
    					when  7 then ISNULL(HoaHongBanGoiDV,0.0) + ISNULL(HoaHongTuVan,0.0)
    						when  8 then ISNULL(HoaHongBanGoiDV,0.0) + ISNULL(HoaHongTuVan,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  9 then ISNULL(HoaHongThucHien,0.0)
    					when  10 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  11 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongBanGoiDV,0.0) 
    					when  12 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongBanGoiDV,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  13 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongTuVan,0.0)
    						when  14 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongTuVan,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    					when  15 then ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongTuVan,0.0) + ISNULL(HoaHongBanGoiDV,0.0) 
    		else ISNULL(HoaHongThucHien,0.0) + ISNULL(HoaHongTuVan,0.0) + ISNULL(HoaHongBanGoiDV,0.0) + ISNULL(HoaHongThucHien_TheoYC,0.0)
    		end as TongAll
			into #tblHoaHong
    		from
    		(
    				select 
							tbl.ID as ID_ChiTietHoaDon,
							tbl.ID_HoaDon,
							tbl.ID_DonViQuiDoi,
							tbl.ID_LoHang,
    						tbl.MaHoaDon,			
    						tbl.LoaiHoaDon,
    						tbl.NgayLapHoaDon,
    						tbl.ID_DoiTuong,
    						tbl.MaHangHoa,
    						tbl.ID_NhanVien,
    						TenHangHoa,
    						CONCAT(TenHangHoa,ThuocTinh_GiaTri) as TenHangHoaFull ,
    						TenDonViTinh,
    						ThuocTinh_GiaTri,
    						TenLoHang,
    						ID_NhomHang,
    						TenNhomHangHoa,
    						SoLuong,
    						--ThanhTien,
    						HeSo,
    						TrangThaiHD,
							
    						tbl.GiaTriTinhCK_NotCP - iif(tbl.LoaiHoaDon=19,0,tbl.TongChiPhiDV) as ThanhTien,

    						case when LoaiHoaDon=6 then - HoaHongThucHien else HoaHongThucHien end as HoaHongThucHien,
    						case when LoaiHoaDon=6 then - PTThucHien else PTThucHien end as PTThucHien,
    						case when LoaiHoaDon=6 then - HoaHongTuVan else HoaHongTuVan end as HoaHongTuVan,
    						case when LoaiHoaDon=6 then - PTTuVan else PTTuVan end as PTTuVan,
    						case when LoaiHoaDon=6 then - PTBanGoi else PTBanGoi end as PTBanGoi,
    						case when LoaiHoaDon=6 then - HoaHongBanGoiDV else HoaHongBanGoiDV end as HoaHongBanGoiDV,
    						case when LoaiHoaDon=6 then - HoaHongThucHien_TheoYC else HoaHongThucHien_TheoYC end as HoaHongThucHien_TheoYC,
    						case when LoaiHoaDon=6 then - PTThucHien_TheoYC else PTThucHien_TheoYC end as PTThucHien_TheoYC
    				from
    				(Select 
						hdct.ID_HoaDon,
						hdct.ID,
    					hd.MaHoaDon,			
    					hd.LoaiHoaDon,
    					hd.NgayLapHoaDon,
    					hd.ID_DoiTuong,
    					dvqd.MaHangHoa,
						hdct.ID_DonViQuiDoi,
						hdct.ID_LoHang,
    					ck.ID_NhanVien,
						hdct.SoLuong,
						IIF(hdct.TenHangHoaThayThe is null or hdct.TenHangHoaThayThe='', hh.TenHangHoa, hdct.TenHangHoaThayThe) as TenHangHoa,
						iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa='1',1,2), hh.LoaiHangHoa) as LoaiHangHoa,    					
    					ISNULL(hh.ID_NhomHang,N'00000000-0000-0000-0000-000000000000') as ID_NhomHang,
    					ISNULL(nhh.TenNhomHangHoa,N'') as TenNhomHangHoa,

						case when hh.ChiPhiTinhTheoPT =1 then hdct.SoLuong * (hdct.DonGia - hdct.TienChietKhau) * hh.ChiPhiThucHien/100
							else hh.ChiPhiThucHien * hdct.SoLuong end as TongChiPhiDV,

						---- gtri cthd (truoc/sau CK)
						case when iif(ck.TinhHoaHongTruocCK is null,0,ck.TinhHoaHongTruocCK) = 1 
							then hdct.SoLuong * hdct.DonGia
							else hdct.SoLuong * (hdct.DonGia - hdct.TienChietKhau)
							end as GiaTriTinhCK_NotCP,

    					ISNULL(dvqd.TenDonVitinh,'')  as TenDonViTinh,
    					ISNULL(lh.MaLoHang,'')  as TenLoHang,
    					ck.HeSo,
    					Case when (dvqd.ThuocTinhGiaTri is null or dvqd.ThuocTinhGiaTri ='') then '' else '_' + dvqd.ThuocTinhGiaTri end as ThuocTinh_GiaTri,
    					Case when ck.ThucHien_TuVan = 1 and TheoYeuCau !=1 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongThucHien,
    						Case when ck.ThucHien_TuVan = 1 and TheoYeuCau !=1 then ISNULL(ck.PT_ChietKhau, 0) else 0 end as PTThucHien,
    					Case when ck.ThucHien_TuVan = 0 and (tinhchietkhautheo is null or tinhchietkhautheo!=4) then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongTuVan,
    					Case when ck.ThucHien_TuVan = 0 and (tinhchietkhautheo is null or tinhchietkhautheo!=4) then ISNULL(ck.PT_ChietKhau, 0) else 0 end as PTTuVan,
    						Case when ck.TinhChietKhauTheo = 4 then ISNULL(ck.PT_ChietKhau, 0) else 0 end as PTBanGoi,
    					Case when ck.TinhChietKhauTheo = 4 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongBanGoiDV,
    					Case when ck.TheoYeuCau = 1 then ISNULL(ck.TienChietKhau, 0) else 0 end as HoaHongThucHien_TheoYC,   				
    					Case when ck.TheoYeuCau = 1 then ISNULL(ck.PT_ChietKhau, 0) else 0 end as PTThucHien_TheoYC,
    						case when hd.ChoThanhToan='0' then 1 else 2 end as TrangThaiHD
    			
    																																		
    				from
    				BH_NhanVienThucHien ck
    				inner join BH_HoaDon_ChiTiet hdct on ck.ID_ChiTietHoaDon = hdct.ID
    				inner join BH_HoaDon hd on hd.ID = hdct.ID_HoaDon
    				inner join DonViQuiDoi dvqd on hdct.ID_DonViQuiDoi = dvqd.ID
    				inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    					left join DM_NhomHangHoa nhh on hh.ID_NhomHang= nhh.ID
    				left join DM_LoHang lh on hdct.ID_LoHang = lh.ID
    				Where hd.ChoThanhToan is not null
    					and hd.ID_DonVi in (select * from dbo.splitstring(@ID_ChiNhanhs))
    					and hd.NgayLapHoaDon >= @DateFrom 
    					and hd.NgayLapHoaDon < @DateTo   							
    					and (exists (select ID from @tblNhanVien nv where ck.ID_NhanVien = nv.ID))
						and (exists (select LoaiChungTu from @tblChungTu ctu where ctu.LoaiChungTu = hd.LoaiHoaDon))
    						and 
    						((select count(Name) from @tblSearchHH b where     									
    							 dvqd.MaHangHoa like '%'+b.Name+'%'
    							or hh.TenHangHoa like '%'+b.Name+'%'
    							or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%'
    							or hh.TenHangHoa_KhongDau like '%'+b.Name+'%'	
    							)=@countHH or @countHH=0)
    			) tbl
				where tbl.LoaiHangHoa in (select LoaiHang from @tblLoaiHang)
    			) tblView
    			join NS_NhanVien nv on tblView.ID_NhanVien= nv.ID
    			left join DM_DoiTuong dt on tblView.ID_DoiTuong= dt.ID		
    			where tblView.TrangThaiHD = @StatusInvoice
    			and exists(select ID from @tblIDNhom a where ID_NhomHang= a.ID)
    			and
    				((select count(Name) from @tblSearchString b where     			
    					nv.TenNhanVien like N'%'+b.Name+'%'
    					or nv.TenNhanVienKhongDau like N'%'+b.Name+'%'
    					or nv.TenNhanVienChuCaiDau like N'%'+b.Name+'%'
    					or nv.MaNhanVien like N'%'+b.Name+'%'	
    					or tblView.MaHoaDon like '%'+b.Name+'%'	
    					)=@count or @count=0)	


				declare @TotalRow int, @TotalPage float, 
					@TongHoaHongThucHien float, @TongHoaHongThucHien_TheoYC float,
					@TongHoaHongTuVan float, @TongHoaHongBanGoiDV float,
					@TongAllAll float, @TongSoLuong float,
					@TongThanhTien float, @TongThanhTienSauHS float

				---- count all row		
				select 
					@TotalRow= count(tbl.ID_HoaDon),
    				@TotalPage= CEILING(COUNT(tbl.ID_HoaDon ) / CAST(@PageSize as float )) ,
    				@TongHoaHongThucHien= sum(HoaHongThucHien) ,
    				@TongHoaHongThucHien_TheoYC = sum(HoaHongThucHien_TheoYC),
    				@TongHoaHongTuVan = sum(HoaHongTuVan),
    				@TongHoaHongBanGoiDV = sum(HoaHongBanGoiDV),
					@TongAllAll = sum(TongAll)
				from #tblHoaHong tbl

				---- sum and group by hoadon + idquydoi
				select 
					@TongSoLuong= sum(SoLuong) ,			   				
    				@TongThanhTien = sum(ThanhTien),
					@TongThanhTienSauHS= sum(GtriSauHeSo) 
				from 
				(
					select  
							tbl.ID_HoaDon,
							tbl.ID_DonViQuiDoi,
							tbl.ID_LoHang,
							max(tbl.SoLuong) as SoLuong,
							max(tbl.ThanhTien) as ThanhTien,
							max(tbl.GtriSauHeSo) as GtriSauHeSo
					from #tblHoaHong tbl
					group by tbl.ID_HoaDon , tbl.ID_DonViQuiDoi ,tbl.ID_LoHang	, tbl.ID_ChiTietHoaDon	
				) tbl
				

				select tbl.*, 
					@TotalRow as TotalRow,
					@TotalPage as TotalPage,
					@TongHoaHongThucHien as TongHoaHongThucHien,
					@TongHoaHongThucHien_TheoYC as TongHoaHongThucHien_TheoYC,
					@TongHoaHongTuVan as TongHoaHongTuVan,
					@TongHoaHongBanGoiDV as TongHoaHongBanGoiDV,
					@TongAllAll as TongAllAll,
					@TongSoLuong as TongSoLuong,
					@TongThanhTien as TongThanhTien,
					@TongThanhTienSauHS as TongThanhTienSauHS
				from #tblHoaHong tbl
				order by tbl.NgayLapHoaDon desc
    			OFFSET (@CurrentPage* @PageSize) ROWS
    			FETCH NEXT @PageSize ROWS ONLY

				
END");

			Sql(@"ALTER PROCEDURE [dbo].[TinhCongNo_HDTra]
	@tblID TblID readonly,
	@LoaiHoaDonTra int
AS
BEGIN
	
	SET NOCOUNT ON;

	declare @LoaiThuChi int = iif(@LoaiHoaDonTra=6,12,11)


	select 
		hd.ID,		
		hd.ID_HoaDon as ID_HoaDonGoc,
		hdd.ID as ID_HoaDonDoi,	
		hd.PhaiThanhToan as HDTra_PhaiThanhToan,	
		hdd.PhaiThanhToan as HDDoi_PhaiThanhToan
	into #tblHD
	from BH_HoaDon hd
	left join BH_HoaDon hdd on hd.ID = hdd.ID_HoaDon and hdd.LoaiHoaDon in (1,19) and hdd.ChoThanhToan='0'
	where exists (select ID from @tblID tbl where hd.ID= tbl.ID)
	
	------ get allHDTra by idHDGoc ----
	select hdt.ID 
	into #allHDTra
	from BH_HoaDon hdt
	where hdt.ChoThanhToan='0' and hdt.LoaiHoaDon= @LoaiHoaDonTra
	and exists (select ID from #tblHD tbl where hdt.ID_HoaDon= tbl.ID_HoaDonGoc ) 


	-------- lũy kế all hdTra + all hdDoi (trước thời điểm trả hàng) ------		
	select hdt.ID, 
		hdt.MaHoaDon,
		hdt.NgayLapHoaDon,
		sum(isnull(hdtBefore.PhaiThanhToan,0)) as HDTra_LuyKeGtriTra,
		sum(isnull(sqChi.TienChi,0)) as HDTra_LuyKeChi,

		sum(isnull(hdDoi.PhaiThanhToan,0)) as HDDoi_LuyKeGiatriDoi,
		sum(isnull(thuDoi.TienChi,0)) as HDDoi_LuyKeThuTien
	into #luykeDoiTra
	from BH_HoaDon hdt
	left join BH_HoaDon hdtBefore on hdt.ID_HoaDon= hdtBefore.ID_HoaDon 
			------- lũy kế trả: chỉ lấy những hdTra trước đó -----
			and hdtBefore.NgayLapHoaDon < hdt.NgayLapHoaDon and hdtBefore.ChoThanhToan='0' and hdtBefore.LoaiHoaDon= @LoaiHoaDonTra
	left join (
		----- get all phieuchi trahang theo hdGoc bandau ----
		select 
			qct.ID_HoaDonLienQuan,
			sum(iif(qhd.LoaiHoaDon= @LoaiThuChi, qct.TienThu, - qct.TienThu)) as TienChi
		from Quy_HoaDon qhd
		join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
		where (qhd.TrangThai is null or qhd.TrangThai='1') 
		and exists (select ID from #allHDTra allTra where allTra.ID= qct.ID_HoaDonLienQuan )
		group by qct.ID_HoaDonLienQuan
	) sqChi on hdtBefore.ID= sqChi.ID_HoaDonLienQuan 
	left join BH_HoaDon hdDoi on hdtBefore.ID = hdDoi.ID_HoaDon and hdDoi.ChoThanhToan='0'
	left join
	(
		----- get all phieuthu hdDoi tu hdTra ----
		select 
			qct.ID_HoaDonLienQuan,
			sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, - qct.TienThu)) as TienChi
		from Quy_HoaDon qhd
		join Quy_HoaDon_ChiTiet qct on qhd.ID= qct.ID_HoaDon
		where (qhd.TrangThai is null or qhd.TrangThai='1') 
		group by qct.ID_HoaDonLienQuan
	) thuDoi on hdDoi.ID= thuDoi.ID_HoaDonLienQuan
	where exists (select ID from #tblHD tbl where hdt.ID_HoaDon= tbl.ID_HoaDonGoc )
	and hdt.LoaiHoaDon= @LoaiHoaDonTra
	group by hdt.ID, hdt.MaHoaDon, hdt.NgayLapHoaDon
	
  
	  ------ tinhcongno hdgoc (chỉ tính công nợ của chính nó)
	  select 
			hdg.ID,
			hdg.ID_BaoGia,
			max(hdg.MaHoaDon) as MaHoaDon,
			max(hdg.LoaiHoaDon) as LoaiHoaDon,	
			max(hdg.PhaiThanhToan) as HDGoc_PhaiThanhToan,
			sum(iif(hdg.LoaiHoaDon = @LoaiThuChi, -hdg.TienThu, hdg.TienThu)) as ThuHDGoc
	  into #tblHDGoc
	  from
	  (
		  ----- thuhdgoc ----
		  select 	
	  		hdg.ID,		
			hdg.ID_HoaDon as ID_BaoGia,
			hdg.MaHoaDon,
			hdg.LoaiHoaDon,
			hdg.PhaiThanhToan,
			qhd.TrangThai,
			isnull(iif(qhd.TrangThai = 0, 0, qct.TienThu),0) as TienThu	 
		  from BH_HoaDon hdg
		  left join Quy_HoaDon_ChiTiet qct on qct.ID_HoaDonLienQuan = hdg.ID and qct.ID_DoiTuong= hdg.ID_DoiTuong
		  left join Quy_HoaDon qhd  on qhd.ID= qct.ID_HoaDon 
		  where  exists (select ID from #tblHD tblHD where hdg.ID= tblHD.ID_HoaDonGoc)
	 ) hdg
	  group by hdg.ID, hdg.ID_BaoGia


	  ---- thu tu dathang (of HDGoc) ----
	  select 
			thuDH.ID,
			thuDH.TienThu as ThuDatHang,
			isFirst	
		into #thuDatHang
		from
		(
		   select 
				hdfromBG.ID,
				hdfromBG.ID_HoaDon,
				hdfromBG.NgayLapHoaDon,
				ROW_NUMBER() OVER(PARTITION BY hdfromBG.ID_HoaDon ORDER BY hdfromBG.NgayLapHoaDon ASC) AS isFirst,	
				sum(iif(qhd.LoaiHoaDon=11, qct.TienThu, -qct.TienThu)) as TienThu
		   from
		   (
			   select 
					hd.ID,
					hd.ID_HoaDon,
					hd.NgayLapHoaDon
			   from dbo.BH_HoaDon hd
			   join dbo.BH_HoaDon hdd on hd.ID_HoaDon= hdd.ID
			   where exists (select ID_BaoGia from #tblHDGoc tblHD where hdd.ID = tblHD.ID_BaoGia)
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



  ----- Cách tính (Phải trả khách) ----
  ----- TH1. Lũy kế tổng trả <= Nợ hóa đơn gốc: Phải trả khách = 0
  ----- Th2. Lũy kế tổng trả < Nợ hóa đơn gốc: Phải trả khách = Tổng trả - chi trả chính nó - công nợ HĐ gốc - Công nợ HD đổi (của HĐ trả)


		select 
			ID,
			MaHoaDonGoc,
			LoaiHoaDonGoc,
			HDDoi_PhaiThanhToan,
			 iif(ID_HoaDonGoc is not null,				
					iif(LuyKeCuoiCung > HDTra_PhaiThanhToan, HDTra_PhaiThanhToan ,
						iif(LuyKeCuoiCung + HDDoi_PhaiThanhToan > HDTra_PhaiThanhToan,HDTra_PhaiThanhToan, LuyKeCuoiCung + HDDoi_PhaiThanhToan)
						), 
				iif(HDDoi_PhaiThanhToan=0,0,iif(HDTra_PhaiThanhToan > HDDoi_PhaiThanhToan, HDTra_PhaiThanhToan - HDDoi_PhaiThanhToan,HDTra_PhaiThanhToan))
				) as BuTruHDGoc_Doi
		from
		(
				select 
					a.ID,
					a.ID_HoaDonGoc,
					a.MaHoaDonGoc,
					a.LoaiHoaDonGoc,
					a.HDDoi_PhaiThanhToan,
					a.HDTra_PhaiThanhToan,					
					a.CongNoHDGoc - a.HDTra_CongNoLuyKe + a.HDDoi_CongNoLuyKe as LuyKeCuoiCung 
				from
				(
				select 
					hdt.ID,		
					hdt.ID_HoaDonGoc,
					hdt.HDTra_PhaiThanhToan,		
					
					isnull(hdgoc.CongNoHDGoc,0) as CongNoHDGoc,	
					isnull(hdgoc.MaHoaDon,'') as MaHoaDonGoc,
					isnull(hdgoc.LoaiHoaDon,0) as LoaiHoaDonGoc,
					isnull(hdt.HDDoi_PhaiThanhToan,0) as HDDoi_PhaiThanhToan,
					
					isnull(lkDoiTra.HDDoi_LuyKeGiatriDoi,0) - isnull(lkDoiTra.HDDoi_LuyKeThuTien,0) as HDDoi_CongNoLuyKe,
					isnull(lkDoiTra.HDTra_LuyKeGtriTra,0) - isnull(lkDoiTra.HDTra_LuyKeChi,0) as HDTra_CongNoLuyKe			
				from #tblHD hdt
				left join #luykeDoiTra lkDoiTra on hdt.ID= lkDoiTra.ID
				left join (
					----- congno HDGoc (bao gồm phiếu thu từ báo giá)
					select 
						hdgoc.ID,		
						hdgoc.MaHoaDon,
						hdgoc.LoaiHoaDon,
						hdgoc.HDGoc_PhaiThanhToan - hdgoc.ThuHDGoc - isnull(thuDH.ThuDatHang,0) as CongNoHDGoc
					from #tblHDGoc hdgoc
					left join #thuDatHang thuDH on hdgoc.ID = thuDH.ID
				) hdgoc on hdt.ID_HoaDonGoc = hdgoc.ID				
			 ) a
		) b

END
");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_NhapXuatTon]
--declare   
	@Text_Search [nvarchar](max) ='',
    @MaHH [nvarchar](max) ='',
    @MaKH [nvarchar](max) ='',
    @MaKH_TV [nvarchar](max) ='',
    @timeStart [datetime] ='2021-01-01',
    @timeEnd [datetime]= '2022-01-01',
    @ID_ChiNhanh [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
    @LaHangHoa [nvarchar](max)='%%',
    @TheoDoi [nvarchar](max)='%1%',
    @TrangThai [nvarchar](max)='%0%',
	@ThoiHan [nvarchar](max)= '%%',
    @ID_NhomHang [nvarchar](max)='%%',
    @ID_NhomHang_SP [nvarchar](max) = ''
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

	declare @tblCTMua_DauKy table(
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
	insert into @tblCTMua_DauKy
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,'2016-01-01',@timeStart

	declare @tblCTMua_GiuaKy table(
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
	insert into @tblCTMua_GiuaKy
	exec BaoCaoGoiDV_GetCTMua @ID_ChiNhanh,@timeStart,@timeEnd


	

	select 
		dvqd.ID_HangHoa,
		dvqd.MaHangHoa,
		hh.TenHangHoa,
		dvqd.TenDonViTinh,
		lh.TenLoHang,
		dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,		
		CONCAT(hh.TenHangHoa, dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
		tblGr.*,
		tblGr.SoLuongBanDK - tblGr.SoLuongSuDungDK - tblGr.SoLuongTraDK as SoLuongConLaiDK,
		tblGr.GiaTriBanDK - tblGr.GiaTriSuDungDK - tblGr.GiaTriTraDK as GiaTriConLaiDK,
		tblGr.SoLuongBanDK - tblGr.SoLuongSuDungDK - tblGr.SoLuongTraDK + tblGr.SoLuongBanGK - tblGr.SoLuongSuDungGK - tblGr.SoLuongTraGK as SoLuongConLaiCK,
		tblGr.GiaTriBanDK - tblGr.GiaTriSuDungDK - tblGr.GiaTriTraDK + tblGr.GiaTriBanGK - tblGr.GiaTriTraGK - tblGr.GiaTriSuDungGK as GiaTriConLaiCK
	from
	(
	select 
		tblUnion.ID_DonViQuiDoi,
		tblUnion.ID_LoHang,		
		tblUnion.ID_DonVi,	
		sum(SoLuongBanDK) as SoLuongBanDK,
		sum(GiaTriBanDK) as GiaTriBanDK,
		sum(SoLuongTraDK) as SoLuongTraDK,
		sum(GiaTriTraDK) as GiaTriTraDK,
		sum(SoLuongSuDungDK) as SoLuongSuDungDK,
		sum(GiaTriSuDungDK) as GiaTriSuDungDK,

		sum(SoLuongBanGK) as SoLuongBanGK,
		sum(GiaTriBanGK) as GiaTriBanGK,
		sum(SoLuongTraGK) as SoLuongTraGK,
		sum(GiaTriTraGK) as GiaTriTraGK,
		sum(SoLuongSuDungGK) as SoLuongSuDungGK,
		sum(GiaTriSuDungGK) as GiaTriSuDungGK
	from
	(
			---- ====== dauky ==== -----
			select 
				tblDK.ID_DonViQuiDoi,
				tblDK.ID_LoHang,
				tblDK.ID_DonVi,
				sum(SoLuongBanDK) as SoLuongBanDK,
				sum(GiaTriBanDK) as GiaTriBanDK,
				sum(SoLuongTra) as SoLuongTraDK,
				sum(GiaTriTra) as GiaTriTraDK,
				sum(SoLuongSuDung) as SoLuongSuDungDK,
				sum(GiaTriSuDung) as GiaTriSuDungDK,

				0 as SoLuongBanGK,
				0 as GiaTriBanGK,
				0 as SoLuongTraGK,
				0 as GiaTriTraGK,
				0 as SoLuongSuDungGK,
				0 as GiaTriSuDungGK
			from
			(
				----- ban dau ky---
				select 
					ctm.ID_DonViQuiDoi,
					ctm.ID_LoHang,
					ctm.ID_DonVi,
					sum(ctm.SoLuong) as SoLuongBanDK,
					sum(ctm.ThanhTien) as GiaTriBanDK,
					0 as SoLuongTra,
					0 as GiaTriTra,
					0 as SoLuongSuDung,
					0 as GiaTriSuDung
				from
				(
					select					
						ctm.ID_DonViQuiDoi,
						ctm.ID_LoHang,
						ctm.ID_DonVi,
						iif(ctm.HanSuDungGoiDV is null,1, iif( GETDATE() < ctm.HanSuDungGoiDV,1,0)) as ThoiHan, --- 1.conhan, 0.hethan				
						ctm.SoLuong,
						ctm.ThanhTien,
						0 as SoLuongTra,
						0 as GiaTriTra,
						0 as SoLuongSuDung,
						0 as GiaTriSuDung
					from  @tblCTMua_DauKy ctm ----- khong where chinhanh o day vi no lau -----			
				) ctm 
				where ctm.ThoiHan like @ThoiHan
				group by ctm.ID_DonViQuiDoi, ctm.ID_LoHang,ctm.ID_DonVi

				union all
				----- tra + sudung (dauky) ----				
					select 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						ct.ID_DonVi,
						0 as SoLuongBanDK,
						0 as GiaTriBanDK,
						sum(ct.SoLuongTra) as SoLuongTra,
						sum(ct.GiaTriTra) as GiaTriTra,
						sum(ct.SoLuongSuDung) as SoLuongSuDung,
						sum(ct.GiaTriSuDung) as GiaTriSuDung
					from
						(
						select 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							iif(ctm.HanSuDungGoiDV is null,1, iif( GETDATE() < ctm.HanSuDungGoiDV,1,0)) as ThoiHan, --- 1.conhan, 0.hethan		
							iif(hd.LoaiHoaDon = 6, ct.SoLuong,0) as SoLuongTra,
							case hd.LoaiHoaDon
								when 6 then iif(hd.TongTienHang =0, 0,  ct.ThanhTien * (1- hd.TongGiamGia/hd.TongTienHang))
							else 0 end as GiaTriTra,
							iif(hd.LoaiHoaDon != 6, ct.SoLuong,0) as SoLuongSuDung,
							iif(hd.LoaiHoaDon != 6, ct.SoLuong * ct.DonGia,0) as GiaTriSuDung
						from BH_HoaDon hd
						join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
						join @tblCTMua_DauKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
						where hd.ChoThanhToan= 0
						and hd.LoaiHoaDon in (1,25,6)
						and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)	
						and ct.ID_ChiTietGoiDV is not null
					) ct
					where ct.ThoiHan like @ThoiHan
					group by ct.ID_DonViQuiDoi, ct.ID_LoHang, ct.ID_DonVi	

			) tblDK	group by tblDK.ID_DonViQuiDoi, tblDK.ID_LoHang, tblDK.ID_DonVi

			union all

			

			------ giua ky ----
			select 
				tblGK.ID_DonViQuiDoi,
				tblGK.ID_LoHang,
				tblGK.ID_DonVi,
				0 as SoLuongBanDK,
				0 as GiaTriBanDK,
				0 as SoLuongTraDK,
				0 as GiaTriTraDK,
				0 as SoLuongSuDungDK,
				0 as GiaTriSuDungDK,

				sum(SoLuongBanGK) as SoLuongBanGK,
				sum(GiaTriBanGK) as GiaTriBanGK,
				sum(SoLuongTra) as SoLuongTraGK,
				sum(GiaTriTra) as GiaTriTraGK,
				sum(SoLuongSuDung) as SoLuongSuDungGK,
				sum(GiaTriSuDung) as GiaTriSuDungGK
			from
			(

			----- ban giua ky ---
				select					
					ctm.ID_DonViQuiDoi,
					ctm.ID_LoHang,
					ctm.ID_DonVi,
					sum(ctm.SoLuong) as SoLuongBanGK,
					sum(ctm.ThanhTien) as GiaTriBanGK,
					0 as SoLuongTra,
					0 as GiaTriTra,
					0 as SoLuongSuDung,
					0 as GiaTriSuDung
				from 
				(
					select					
						ctm.ID_DonViQuiDoi,
						ctm.ID_LoHang,
						ctm.ID_DonVi,
						iif(ctm.HanSuDungGoiDV is null,1, iif( GETDATE() < ctm.HanSuDungGoiDV,1,0)) as ThoiHan, --- 1.conhan, 0.hethan				
						ctm.SoLuong,
						ctm.ThanhTien,
						0 as SoLuongTra,
						0 as GiaTriTra,
						0 as SoLuongSuDung,
						0 as GiaTriSuDung
					from  @tblCTMua_GiuaKy ctm
				) ctm
				where ctm.ThoiHan like @ThoiHan
				group by ctm.ID_DonViQuiDoi, ctm.ID_LoHang,ctm.ID_DonVi

				union all

				----- tra + sudung (giua ky) ----

				select 
					ct.ID_DonViQuiDoi,
					ct.ID_LoHang,
					ct.ID_DonVi,
					0 as SoLuongBanGK,
					0 as GiaTriBanGK,
					sum(ct.SoLuongTra) as SoLuongTra,
					sum(ct.GiaTriTra) as GiaTriTra,
					sum(ct.SoLuongSuDung) as SoLuongSuDung,
					sum(ct.GiaTriSuDung) as GiaTriSuDung
				from
					(
					select 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						iif(ctm.HanSuDungGoiDV is null,1, iif( GETDATE() < ctm.HanSuDungGoiDV,1,0)) as ThoiHan, --- 1.conhan, 0.hethan		
						iif(hd.LoaiHoaDon = 6, ct.SoLuong,0) as SoLuongTra,
						case hd.LoaiHoaDon
							when 6 then iif(hd.TongTienHang =0, 0,  ct.ThanhTien * (1- hd.TongGiamGia/hd.TongTienHang))
						else 0 end as GiaTriTra,
						iif(hd.LoaiHoaDon != 6, ct.SoLuong,0) as SoLuongSuDung,
						iif(hd.LoaiHoaDon != 6, ct.SoLuong * ct.DonGia,0) as GiaTriSuDung
					from BH_HoaDon hd
					join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
					join @tblCTMua_GiuaKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
					where hd.ChoThanhToan= 0
					and hd.LoaiHoaDon in (1,25,6)
					and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)	
					and ct.ID_ChiTietGoiDV is not null
				) ct
				where ct.ThoiHan like @ThoiHan
				group by ct.ID_DonViQuiDoi, ct.ID_LoHang, ct.ID_DonVi	
				
			) tblGK	group by tblGK.ID_DonViQuiDoi, tblGK.ID_LoHang	,tblGK.ID_DonVi							
	) tblUnion
	group by tblUnion.ID_DonViQuiDoi,tblUnion.ID_LoHang,tblUnion.ID_DonVi
	)tblGr
	join DonViQuiDoi dvqd on tblGr.ID_DonViQuiDoi = dvqd.ID
    join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    left join DM_LoHang lh on tblGr.ID_LoHang = lh.ID	
    where  hh.LaHangHoa like @LaHangHoa
    and hh.TheoDoi like @TheoDoi
	and dvqd.Xoa like @TrangThai
	and exists (select * from @tblChiNhanh cn where cn.ID_DonVi= tblGr.ID_DonVi)
    and (dvqd.MaHangHoa like @Text_Search or dvqd.MaHangHoa like @MaHH or hh.TenHangHoa_KhongDau like @MaHH or hh.TenHangHoa_KyTuDau like @MaHH)
END");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDonTraHang]
--declare
     @timeStart [datetime] ='2023-01-01',
    @timeEnd [datetime]= '2024-01-01',
    @ID_ChiNhanh [nvarchar](max) ='d93b17ea-89b9-4ecf-b242-d03b8cde71de',
    @maHD [nvarchar](max)='',
	@ID_NhanVienLogin uniqueidentifier ='50d93282-dbf7-4084-8832-ea6ce9bd67b2',
	@NguoiTao nvarchar(max)='admin',
	@TrangThai nvarchar(max)='0,1,2,3',
	@ColumnSort varchar(max),
	@SortBy varchar(max)='NgayLapHoaDon',
	@CurrentPage int = 0,
	@PageSize int = 50
AS
BEGIN
	set nocount on;
	declare @tblNhanVien table (ID uniqueidentifier)
	insert into @tblNhanVien
	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanh,'TraHang_XemDS_PhongBan','TraHang_XemDS_PhongBan');

	declare @tblChiNhanh table (ID varchar(40))
	insert into @tblChiNhanh
	select Name from dbo.splitstring(@ID_ChiNhanh)

	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@maHD, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch)


	;with data_cte
	as(
	
    SELECT 
    	c.ID,
    	c.ID_BangGia,
    	c.ID_HoaDon,
		c.ID_Xe,
    	c.LoaiHoaDon,
    	c.ID_ViTri,
    	c.ID_DonVi,
    	c.ID_NhanVien,
    	c.ID_DoiTuong,		
    	c.ChoThanhToan,
    	c.MaHoaDon,
    	c.BienSo,
    	c.NgayLapHoaDon,
    	c.TenDoiTuong,
		ISNULL(c.MaDoiTuong,'') as MaDoiTuong,
    	ISNULL(c.NguoiTaoHD,'') as NguoiTaoHD,
		c.DienThoai,
		c.Email,
		c.DiaChiKhachHang,
		c.NgaySinh_NgayTLap,
    	c.TenDonVi,
    	c.TenNhanVien,
    	c.DienGiai,
    	c.TenBangGia,
    	c.TongTienHang, c.TongGiamGia,
		c.KhuyeMai_GiamGia,
		c.PhaiThanhToan,		
		c.TongChiPhi,
		c.KhachDaTra, 
		c.TongThanhToan,
		c.ThuTuThe,
		c.TienMat,
		c.ChuyenKhoan,
		c.TongChietKhau,c.TongTienThue,
    	c.TrangThai,
    	c.TheoDoi,
    	c.TenPhongBan,
    	c.DienThoaiChiNhanh,
    	c.DiaChiChiNhanh,
    	c.DiemGiaoDich,
		c.ID_BaoHiem, c.ID_PhieuTiepNhan,
		c.TongTienBHDuyet, PTThueHoaDon, c.PTThueBaoHiem, c.TongTienThueBaoHiem, c.SoVuBaoHiem,
		c.KhauTruTheoVu, c.PTGiamTruBoiThuong,
		c.GiamTruBoiThuong, c.BHThanhToanTruocThue,
		c.PhaiThanhToanBaoHiem,		
		c.MaHoaDonGoc,
    	'' as HoaDon_HangHoa -- string contail all MaHangHoa,TenHangHoa of HoaDon
    	FROM
    	(
    		select 
    	
    		a.ID as ID,
    		bhhd.MaHoaDon,
    		bhhd.LoaiHoaDon,
    		bhhd.ID_BangGia,
    		bhhd.ID_HoaDon,
    		bhhd.ID_ViTri,
    		bhhd.ID_DonVi,
    		bhhd.ID_NhanVien,
    		bhhd.ID_DoiTuong,
			bhhd.NgayLapHoaDon,
			bhhd.ChoThanhToan,
    		ISNULL(bhhd.DiemGiaoDich,0) as DiemGiaoDich,   		
    		ISNULL(vt.TenViTri,'') as TenPhongBan,
    		ISNULL(hdg.MaHoaDon,'') as MaHoaDonGoc,
    		CASE 
    			WHEN dt.TheoDoi IS NULL THEN 
    				CASE WHEN dt.ID IS NULL THEN '0' ELSE '1' END
    			ELSE dt.TheoDoi
    		END AS TheoDoi,

			dt.MaDoiTuong,
			ISNULL(dt.TenDoiTuong, N'Khách lẻ') as TenDoiTuong,
			ISNULL(dt.TenDoiTuong_KhongDau, N'Khách lẻ') as TenDoiTuong_KhongDau,
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
    		CAST(ROUND(bhhd.TongTienHang, 0) as float) as TongTienHang,
    		CAST(ROUND(bhhd.TongGiamGia, 0) as float) as TongGiamGia,
			isnull(bhhd.KhuyeMai_GiamGia,0) as KhuyeMai_GiamGia,
    		CAST(ROUND(bhhd.TongChiPhi, 0) as float) as TongChiPhi,
    		CAST(ROUND(bhhd.PhaiThanhToan, 0) as float) as PhaiThanhToan,
			CAST(ROUND(bhhd.TongTienThue, 0) as float) as TongTienThue,
			isnull(bhhd.TongThanhToan, bhhd.PhaiThanhToan) as TongThanhToan,

			bhhd.ID_BaoHiem, bhhd.ID_PhieuTiepNhan,bhhd.ID_Xe,
			xe.BienSo,
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
    		a.KhachDaTra,
    		a.ThuTuThe,
    		a.TienMat,
    		a.ChuyenKhoan,
    		bhhd.TongChietKhau,			
			case bhhd.ChoThanhToan
				when 0 then 0
				when 1 then 1
				else 4 end as TrangThaiHD,   
    		Case When bhhd.ChoThanhToan = 0 then N'Hoàn thành' else N'Đã hủy' end as TrangThai
    		FROM
    		(
    			select a1.ID, 
					sum(KhachDaTra) as KhachDaTra,
					sum(ThuTuThe) as ThuTuThe,
					sum(TienMat) as TienMat,
					sum(TienPOS) as TienATM,
					sum(TienCK) as ChuyenKhoan
				from (
					Select 
    				bhhd.ID,					
					case when qhd.TrangThai ='0' then 0 else ISNULL(qct.Tienthu, 0) end as KhachDaTra,
					Case when qhd.TrangThai = 0 then 0 else iif(qct.HinhThucThanhToan=4, isnull(qct.TienThu,0),0) end as ThuTuThe,
					case when qhd.TrangThai = 0 then 0 else iif(qct.HinhThucThanhToan=1, isnull(qct.TienThu,0),0) end as TienMat,										
					case when qhd.TrangThai = 0 then 0 else iif(qct.HinhThucThanhToan=2, isnull(qct.TienThu,0),0) end as TienPOS,
					case when qhd.TrangThai = 0 then 0 else iif(qct.HinhThucThanhToan=3, isnull(qct.TienThu,0),0) end as TienCK							
    				from BH_HoaDon bhhd
    				left join Quy_HoaDon_ChiTiet qct on bhhd.ID = qct.ID_HoaDonLienQuan	
    				left join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID					
    				where bhhd.LoaiHoaDon = 6
					and bhhd.NgayLapHoadon between  @timeStart and @timeEnd 
					and bhhd.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
				) a1 group by a1.ID
    		) as a
    		left join BH_HoaDon bhhd on a.ID = bhhd.ID	
			left join BH_HoaDon hdg on bhhd.ID_HoaDon = hdg.ID
    		left join DM_DoiTuong dt on bhhd.ID_DoiTuong = dt.ID
    		left join DM_DonVi dv on bhhd.ID_DonVi = dv.ID
    		left join NS_NhanVien nv on bhhd.ID_NhanVien = nv.ID 
    		left join DM_TinhThanh tt on dt.ID_TinhThanh = tt.ID
    		left join DM_QuanHuyen qh on dt.ID_QuanHuyen = qh.ID
    		left join DM_GiaBan gb on bhhd.ID_BangGia = gb.ID
    		left join DM_ViTri vt on bhhd.ID_ViTri = vt.ID    		
			left join Gara_DanhMucXe xe on bhhd.ID_Xe = xe.ID
    		) as c
			join (select Name from dbo.splitstring(@TrangThai)) tt on c.TrangThaiHD = tt.Name
			where (exists( select * from @tblNhanVien nv where nv.ID= c.ID_NhanVien) or c.NguoiTaoHD= @NguoiTao)
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
				or c.MaHoaDonGoc like '%'+b.Name+'%'	
				)=@count or @count=0)	
			), 
			count_cte
		as (
			select count(ID) as TotalRow,
				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
				sum(TongTienHang) as SumTongTienHang,
				sum(TongGiamGia) as SumTongGiamGia,
				sum(KhachDaTra) as SumKhachDaTra,	
				sum(PhaiThanhToan) as SumPhaiThanhToan,			
				sum(TongChiPhi) as SumTongChiPhi,				
				sum(ThuTuThe) as SumThuTuThe,				
				sum(TienMat) as SumTienMat,			
				sum(ChuyenKhoan) as SumChuyenKhoan,				
				sum(TongTienThue) as SumTongTienThue
			from data_cte
		),
		tblView as
		(
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
    	)
		----- select top 10 -----
		select *
		into #tblView
		from tblView

		----- get list ID of top 10
		declare @tblID TblID
		insert into @tblID
		select ID from #tblView
		
		------ get congno of top 10
		declare @tblCongNo table (ID uniqueidentifier, MaHoaDonGoc nvarchar(max), LoaiHoaDonGoc int, HDDoi_PhaiThanhToan float, BuTruHDGoc_Doi float)
		insert into @tblCongNo
		exec TinhCongNo_HDTra @tblID, 6
					
		
		select tView.*,
			cn.MaHoaDonGoc,
			cn.LoaiHoaDonGoc,
			isnull(cn.BuTruHDGoc_Doi,0) as TongTienHDDoiTra,
			tView.PhaiThanhToan - isnull(cn.BuTruHDGoc_Doi,0) as TongTienHDTra, --- muontruong: PhaiTraKhach (sau khi butru congno hdGoc & hdDoi)
			tView.PhaiThanhToan - isnull(cn.BuTruHDGoc_Doi,0) - tView.KhachDaTra as ConNo
		from #tblView tView
		left join @tblCongNo cn on tView.ID = cn.ID
		order by tView.NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[PhieuTiepNhan_GetThongTinChiTiet]
    @ID_PhieuTiepNhan [uniqueidentifier]
AS
BEGIN
    SET NOCOUNT ON;	
    	
    	select tn.*,		
    		xe.BienSo, xe.SoKhung, xe.SoMay,
    		xe.DungTich, xe.HopSo, xe.MauSon, xe.NamSanXuat, 
    		nvlap.TenNhanVien as NhanVienTiepNhan,
    		nvlap.MaNhanVien as MaNVTiepNhan,
    		ISNULL(nv.TenNhanVien,'') as CoVanDichVu,
    		ISNULL(cv.DienThoaiDiDong,'') as CoVan_SDT,
    		ISNULL(nv.MaNhanVien,'') as MaCoVan,
    		ISNULL(nv.TenNhanVienKhongDau,'') as TenNhanVienKhongDau,
    		ISNULL(dt.MaDoiTuong,'') as MaDoiTuong,
    		isnull(dt.TenDoiTuong,'') as TenDoiTuong,
    		isnull(dt.TenDoiTuong_KhongDau,'') as TenDoiTuong_KhongDau,
    		dt.DienThoai as DienThoaiKhachHang,
    		dt.DiaChi,
    		dt.Email,
    		cast(iif(xe.ID_KhachHang = tn.ID_KhachHang,'1','0') as bit) as LaChuXe,
			xe.ID_KhachHang as ID_ChuXe,
    		cx.TenDoiTuong as ChuXe,
    		cx.DienThoai as ChuXe_SDT,
    		cx.Email as ChuXe_Email,
    		cx.DiaChi as ChuXe_DiaChi,
    		mau.TenMauXe,
    		hang.TenHangXe,
    		loai.TenLoaiXe,
			tn.ID_BaoHiem,
			tn.NguoiLienHeBH,
			tn.SoDienThoaiLienHeBH,
			ISNULL(bh.TenDoituong,'') as TenBaoHiem,
			ISNULL(bh.MaDoiTuong,'') as MaBaoHiem   	
    	from Gara_PhieuTiepNhan tn
    	join Gara_DanhMucXe xe on tn.ID_Xe = xe.ID
    	join Gara_MauXe mau on xe.ID_MauXe = mau.ID
    	join Gara_HangXe hang on mau.ID_HangXe= hang.ID
    	join Gara_LoaiXe loai on mau.ID_LoaiXe= loai.ID
    	join NS_NhanVien nvlap on tn.ID_NhanVien= nvlap.ID
    	left join NS_NhanVien cv on tn.ID_CoVanDichVu= cv.ID
    	left join DM_DoiTuong dt on tn.ID_KhachHang = dt.ID
    	left join DM_DoiTuong cx on xe.ID_KhachHang = cx.ID
		left join DM_DoiTuong bh on tn.ID_BaoHiem= bh.ID
    	left join NS_NhanVien nv on tn.ID_CoVanDichVu= nv.ID
    	where tn.id= @ID_PhieuTiepNhan
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
    	left join DM_NhomHangHoa nhh on hh.ID_NhomHang = nhh.ID 
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapXuatTon]
	@ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
    @timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
    @CoPhatSinh [int]
AS
BEGIN

	SET NOCOUNT ON;

    DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER, MaDonVi nvarchar(max), TenDonVi nvarchar(max));
    INSERT INTO @tblChiNhanh 
	SELECT dv.ID, dv.MaDonVi, dv.TenDonVi 
	FROM splitstring(@ID_DonVi) cn
	join DM_DonVi  dv on cn.Name= dv.ID

	declare @dtNow datetime = format(getdate(),'yyyy-MM-dd')  

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
	exec dbo.GetAll_TonKhoDauKy @ID_DonVi, @timeStart

	

			------ tonkho trongky
			select 			
				qd.ID_HangHoa,
				tkNhapXuat.ID_LoHang,
				tkNhapXuat.ID_DonVi,				
				sum(tkNhapXuat.SoLuongNhap * qd.TyLeChuyenDoi) as SoLuongNhap,
				sum(tkNhapXuat.GiaTriNhap ) as GiaTriNhap,
				sum(tkNhapXuat.SoLuongXuat * qd.TyLeChuyenDoi) as SoLuongXuat,
				sum(tkNhapXuat.GiaTriXuat) as GiaTriXuat
				into #temp
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
				and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
				or (hd.LoaiHoaDon = 10  and (hd.YeuCau='1' or hd.YeuCau='4')) )
				AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
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
				and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
				and hd.ID_CheckIn in (select ID from @tblChiNhanh)
				AND hd.NgaySua between  @timeStart AND   @timeEnd
				GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn

    			UNION ALL
				 ---nhaphang + khach trahang
				SELECT 
					ct.ID_DonViQuiDoi,
					ct.ID_LoHang,
					hd.ID_DonVi,
					SUM(ct.SoLuong) AS SoLuongNhap,
					--- KH trahang: giatrinhap = giavon (khong lay giaban)
					sum(case hd.LoaiHoaDon
						when 6 then iif(ctm.GiaVon is null or ctm.ID = ctm.ID_ChiTietDinhLuong, ct.SoLuong * ct.GiaVon, ct.SoLuong *ctm.GiaVon)
						when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang)
															------ + chiphi nhap -----
						                                       + ct.ThanhTien * (hd.TongChiPhi/hd.TongTienHang))
					else ct.SoLuong * ct.GiaVon end) as GiaTriNhap,
					0 AS SoLuongXuat,
					0 AS GiaTriXuat
				FROM BH_HoaDon_ChiTiet ct
				LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
				left join BH_HoaDon_ChiTiet ctm on ct.ID_ChiTietGoiDV = ctm.ID
				WHERE hd.LoaiHoaDon in (4,6,13,14)
				AND hd.ChoThanhToan = 0
				and (ct.ChatLieu is null or ct.ChatLieu not in ('2','5'))
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
				AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd
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
				and hd.ID_DonVi in (select ID from @tblChiNhanh)
    			AND hd.NgayLapHoaDon between  @timeStart AND   @timeEnd  			
				) ctkk	
    			GROUP BY ctkk.ID_DonViQuiDoi, ctkk.ID_LoHang, ctkk.ID_DonVi
			)tkNhapXuat
			join DonViQuiDoi qd on tkNhapXuat.ID_DonViQuiDoi = qd.ID
			group by qd.ID_HangHoa, tkNhapXuat.ID_LoHang, tkNhapXuat.ID_DonVi


			if	@CoPhatSinh= 2
					begin
							select 
							a.TenNhomHang,
							a.TenHangHoa,
							a.MaHangHoa,
							a.TenDonViTinh,
							a.TenLoHang,
							a.TenDonVi,
							a.MaDonVi,
							concat(a.TenHangHoa,a.ThuocTinhGiaTri) as TenHangHoaFull,
							-- dauky
							isnull(a.TonDauKy,0) as TonDauKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0),0) as GiaTriDauKy,

							--- trongky
							isnull(a.SoLuongNhap,0) as SoLuongNhap,
							iif(@XemGiaVon='1',isnull(a.GiaTriNhap,0),0) as GiaTriNhap,
							isnull(a.SoLuongXuat,0) as SoLuongXuat,
							iif(@XemGiaVon='1',isnull(a.GiaTriXuat,0),0) as GiaTriXuat,

							-- cuoiky
							isnull(a.TonDauKy,0) + isnull(a.SoLuongNhap,0) - isnull(a.SoLuongXuat,0) as TonCuoiKy,
							(isnull(a.TonDauKy,0) + isnull(a.SoLuongNhap,0) - isnull(a.SoLuongXuat,0)) * iif(a.QuyCach=0 or a.QuyCach is null,1, a.QuyCach)  as TonQuyCach,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0) + isnull(a.GiaTriNhap,0) - isnull(a.GiaTriXuat,0),0)  as GiaTriCuoiKy
						from
						(
							select 
								isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
								hh.TenHangHoa,
								hh.QuyCach,
								qd.MaHangHoa,
								qd.TenDonViTinh,
								isnull(lo.MaLoHang,'') as TenLoHang,
								dv.TenDonVi,
								dv.MaDonVi,
								qd.ThuocTinhGiaTri,
				
								-- dauky	

								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) as TonDauKy,		
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) * iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaTriDauKy,
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaVon,	
							
									----trongky
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap, 0) as SoLuongNhap,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat, 0) as SoLuongXuat,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTriNhap, 0) as GiaTriNhap,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTriXuat, 0) as GiaTriXuat
								
							from #temp tkTrongKy
							left join DM_HangHoa hh on tkTrongKy.ID_HangHoa= hh.ID
							left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
							left join DonViQuiDoi qd on tkTrongKy.ID_HangHoa = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
							left join DM_LoHang lo on tkTrongKy.ID_LoHang= lo.ID or lo.ID is null
							cross join @tblChiNhanh  dv									
							left join @tkDauKy tkDauKy on hh.ID = tkDauKy.ID_HangHoa 
							and tkTrongKy.ID_DonVi= tkDauKy.ID_DonVi and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null and hh.QuanLyTheoLoHang = 0 ))							
							where hh.LaHangHoa= 1
								AND hh.TheoDoi LIKE @TheoDoi	
								and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
								and dv.ID in (select ID from @tblChiNhanh)
									AND ((select count(Name) from @tblSearchString b where 
    								hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    								or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    									or hh.TenHangHoa like '%'+b.Name+'%'
    									or lo.MaLoHang like '%' +b.Name +'%' 
    									or qd.MaHangHoa like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    									or qd.TenDonViTinh like '%'+b.Name+'%'
    									or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
										or dv.MaDonVi like '%'+b.Name+'%'
										or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)		
							) a	order by TenHangHoa, TenDonVi,TenLoHang		
					end
			else
			begin
			
					select 	
							dv.MaDonVi, dv.TenDonVi,
							qd.MaHangHoa,
							hh.TenHangHoa,
							hh.QuyCach,
							lo.ID as ID_LoHang,
							qd.ID as ID_DonViQuyDoi,
							concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
							isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
							isnull(qd.TenDonViTinh,'') as TenDonViTinh,
							isnull(lo.MaLoHang,'') as TenLoHang,

							---- dauky
							isnull(tkDauKy.TonKho,0) as TonDauKy,
							isnull(tkDauKy.GiaVon,0) as GiaVon,				
							iif(@XemGiaVon='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0),0)  as GiaTriDauKy,			
							isnull(nhom.TenNhomHangHoa,N'Nhóm Hàng Hóa Mặc Định') TenNhomHang,

							---- trongky
							isnull(tkTrongKy.SoLuongNhap,0) as SoLuongNhap,
							isnull(tkTrongKy.SoLuongXuat,0) as SoLuongXuat,
							iif(@XemGiaVon='1',isnull(tkTrongKy.GiaTriNhap,0),0) as GiaTriNhap,
							iif(@XemGiaVon='1',isnull(tkTrongKy.GiaTriXuat,0),0) as GiaTriXuat,

							---- cuoiky
							isnull(tkDauKy.TonKho,0) + isnull(tkTrongKy.SoLuongNhap,0) - isnull(tkTrongKy.SoLuongXuat,0) as TonCuoiKy,
							(isnull(tkDauKy.TonKho,0) + isnull(tkTrongKy.SoLuongNhap,0) - isnull(tkTrongKy.SoLuongXuat,0))  * iif(hh.QuyCach=0 or hh.QuyCach is null,1, hh.QuyCach) as TonQuyCach,
							iif(@XemGiaVon='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0) + isnull(tkTrongKy.GiaTriNhap,0)
							- isnull(tkTrongKy.GiaTriXuat,0),0) as GiaTriCuoiKy
					from DM_HangHoa hh 		
					join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
					left join DM_LoHang lo on hh.ID = lo.ID_HangHoa
					left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
					cross join @tblChiNhanh  dv		
					left join @tkDauKy tkDauKy 
					on qd.ID_HangHoa = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= dv.ID and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
					left join #temp tkTrongKy on qd.ID_HangHoa = tkTrongKy.ID_HangHoa and tkTrongKy.ID_DonVi= dv.ID and ((lo.ID= tkTrongKy.ID_LoHang) or (lo.ID is null ))
					where hh.LaHangHoa= 1
					AND hh.TheoDoi LIKE @TheoDoi	
					and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
					and dv.ID in (select ID from @tblChiNhanh)
						AND ((select count(Name) from @tblSearchString b where 
    					hh.TenHangHoa_KhongDau like '%'+b.Name+'%' 
    					or hh.TenHangHoa_KyTuDau like '%'+b.Name+'%' 
    						or hh.TenHangHoa like '%'+b.Name+'%'
    						or lo.MaLoHang like '%' +b.Name +'%' 
    						or qd.MaHangHoa like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KhongDau like '%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KyTuDau like '%'+b.Name+'%'
    						or qd.TenDonViTinh like '%'+b.Name+'%'
    						or qd.ThuocTinhGiaTri like '%'+b.Name+'%'
							or dv.MaDonVi like '%'+b.Name+'%'
							or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)		
						order by TenHangHoa, TenDonVi,MaLoHang
			end

			
			
END");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_NhapXuatTonChiTiet]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
    @timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
    @CoPhatSinh [int]
AS
BEGIN
    SET NOCOUNT ON;
	DECLARE @tblIdDonVi TABLE (ID UNIQUEIDENTIFIER, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX));
	INSERT INTO @tblIdDonVi
	SELECT donviinput.Name, dv.MaDonVi, dv.TenDonVi FROM [dbo].[splitstring](@ID_DonVi) donviinput
	INNER JOIN DM_DonVi dv
	ON dv.ID = donviinput.Name;

	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@ID_DonVi)

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

	declare @tkDauKy table (ID_DonVi uniqueidentifier,ID_HangHoa uniqueidentifier,	ID_LoHang uniqueidentifier null, TonKho float,GiaVon float)		
	insert into @tkDauKy
	exec dbo.GetAll_TonKhoDauKy @ID_DonVi, @timeStart

	---- phatsinh trongky
	select 
		qd.ID_HangHoa,	
		tblQD.ID_LoHang,
		tblQD.ID_DonVi,
		sum(SoLuongNhap_NCC * qd.TyLeChuyenDoi) as SoLuongNhap_NCC,
		sum(SoLuongNhap_Kiem * qd.TyLeChuyenDoi) as SoLuongNhap_Kiem,
		sum(SoLuongNhap_Tra * qd.TyLeChuyenDoi) as SoLuongNhap_Tra,
		sum(SoLuongNhap_Chuyen * qd.TyLeChuyenDoi) as SoLuongNhap_Chuyen,

		sum(SoLuongXuat_Ban * qd.TyLeChuyenDoi) as SoLuongXuat_Ban,
		sum(SoLuongXuat_Huy * qd.TyLeChuyenDoi) as SoLuongXuat_Huy,
		sum(SoLuongXuat_NCC * qd.TyLeChuyenDoi) as SoLuongXuat_NCC,
		sum(SoLuongXuat_Kiem * qd.TyLeChuyenDoi) as SoLuongXuat_Kiem,
		sum(SoLuongXuat_Chuyen * qd.TyLeChuyenDoi) as SoLuongXuat_Chuyen,		
		sum(GiaTri) as GiaTri
		into #temp
	from
	(
		select 
			tblHD.ID_DonViQuiDoi,
			tblHD.ID_LoHang,
			tblHD.ID_DonVi,
			SUM(tblHD.SoLuongNhap_NCC) AS SoLuongNhap_NCC,
			SUM(tblHD.SoLuongNhap_Kiem) AS SoLuongNhap_Kiem,
			SUM(tblHD.SoLuongNhap_Tra) AS SoLuongNhap_Tra,
			SUM(tblHD.SoLuongNhap_Chuyen) AS SoLuongNhap_Chuyen,
		
			SUM(tblHD.SoLuongXuat_Ban) AS SoLuongXuat_Ban,
			SUM(tblHD.SoLuongXuat_Huy) AS SoLuongXuat_Huy,
			SUM(tblHD.SoLuongXuat_NCC) AS SoLuongXuat_NCC,
			SUM(tblHD.SoLuongXuat_Kiem) AS SoLuongXuat_Kiem,
			SUM(tblHD.SoLuongXuat_Chuyen) AS SoLuongXuat_Chuyen,		
			SUM(tblHD.GiaTri) AS GiaTri
		from
		(
				-----  banhang, xuatkho, trancc, kiem -, chuyenhang
				-- xuatban
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						SUM(ct.SoLuong) AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem,
						0 AS SoLuongXuat_Chuyen,					
						SUM(ct.SoLuong * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (1)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuatkho
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						SUM(ct.SoLuong) AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,					
						SUM(ct.SoLuong * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (8)
					and hd.ChoThanhToan = 0 
					and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuat traNCC
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						SUM(ct.SoLuong) AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,						
						SUM(ct.SoLuong * ct.DonGia) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (7)
					and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

					union all
					-- xuat/nhap kiemke
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						 0 AS SoLuongNhap_NCC,
    					sum(IIF(ct.SoLuong >= 0, ct.SoLuong, 0)) AS SoLuongNhap_Kiem, 
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
    					sum(IIF(ct.SoLuong < 0, ct.SoLuong *(-1), 0)) AS SoLuongXuat_Kiem,
						0 AS SoLuongXuat_Chuyen,						
						sum((ct.ThanhTien - ct.TienChietKhau) * ct.GiaVon) as GiaTri -- soluongthucte - soluongDB (if > 0:nhap else xuat)
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon in (9)
					and hd.ChoThanhToan = 0 
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

				
					union all
					-- xuat chuyenhang
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						0 AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						SUM(ct.TienChietKhau) AS SoLuongXuat_Chuyen,						
						SUM(ct.TienChietKhau * ct.GiaVon) * (-1) AS GiaTri
					FROM BH_HoaDon_ChiTiet ct
					JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.ChoThanhToan = 0 and
						((hd.LoaiHoaDon = 10 and hd.yeucau = '1') 
						OR (hd.LoaiHoaDon = 10 and hd.YeuCau = '4'))  
					and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
					and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
					AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

						---- nhaphang, kiemhang, kh tra, nhanhang
					union all
					-- nhaphang
					SELECT 
						ct.ID_DonViQuiDoi,
						ct.ID_LoHang,
						hd.ID_DonVi,
						SUM(ct.SoLuong) AS SoLuongNhap_NCC,
						0 AS SoLuongNhap_Kiem,
						0 AS SoLuongNhap_Tra,
						0 AS SoLuongNhap_Chuyen,
						
						0 AS SoLuongXuat_Ban,
						0 AS SoLuongXuat_Huy,
						0 AS SoLuongXuat_NCC,
						0 AS SoLuongXuat_Kiem, 
						0 AS SoLuongXuat_Chuyen,		
						sum(case hd.LoaiHoaDon						
							when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang)
																------ + chiphi nhap -----
						                                       + ct.ThanhTien * (hd.TongChiPhi/hd.TongTienHang))
							else ct.SoLuong * ct.GiaVon end) as GiaTri
						FROM BH_HoaDon_ChiTiet ct   
						JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						WHERE hd.LoaiHoaDon in (4,13,14)
						and hd.ChoThanhToan = 0 
						and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi


						union all
						-- kh trahang
						SELECT 
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							0 AS SoLuongNhap_NCC,
							0 AS SoLuongNhap_Kiem,
							SUM(ct.SoLuong) AS SoLuongNhap_Tra,
							0 AS SoLuongNhap_Chuyen,
							
							0 AS SoLuongXuat_Ban,
							0 AS SoLuongXuat_Huy,
							0 AS SoLuongXuat_NCC,
							0 AS SoLuongXuat_Kiem, 
							0 AS SoLuongXuat_Chuyen,							
							SUM(ct.SoLuong * iif(ctm.GiaVon is null, ct.GiaVon,ctm.GiaVon))  AS GiaTri
						FROM BH_HoaDon_ChiTiet ct   
						JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
						left join BH_HoaDon_ChiTiet ctm on ct.ID_ChiTietGoiDV = ctm.ID
						WHERE hd.LoaiHoaDon = 6  and hd.ChoThanhToan = 0
						and (ct.ChatLieu is null or ct.ChatLieu !='2')
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_DonVi

						union all
						-- nhan chuyenhang
						select
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_CheckIn,
							0 AS SoLuongNhap_NCC,
							0 AS SoLuongNhap_Kiem,
							0 AS SoLuongNhap_Tra,
							SUM(ct.TienChietKhau) AS SoLuongNhap_Chuyen,
							
							0 AS SoLuongXuat_Ban,
							0 AS SoLuongXuat_Huy,
							0 AS SoLuongXuat_NCC,
							0 AS SoLuongXuat_Kiem, 
							0 AS SoLuongXuat_Chuyen,							
							SUM(ct.TienChietKhau * ct.DonGia) AS GiaTri -- lay dongia cua ben chuyen
					FROM BH_HoaDon_ChiTiet ct
					LEFT JOIN BH_HoaDon hd ON ct.ID_HoaDon = hd.ID
					WHERE hd.LoaiHoaDon = 10 and hd.YeuCau = '4' AND hd.ChoThanhToan = 0
					and (ct.ChatLieu is null or ct.ChatLieu not in ('5'))
					and exists (select ID from @tblChiNhanh dv where hd.ID_CheckIn = dv.ID)
					AND hd.NgaySua between @timeStart AND  @timeEnd
					GROUP BY ct.ID_DonViQuiDoi, ct.ID_LoHang, hd.ID_CheckIn
			)tblHD
			group by tblHD.ID_DonViQuiDoi,tblHD.ID_LoHang,tblHD.ID_DonVi
		)tblQD
		join DonViQuiDoi qd on tblQD.ID_DonViQuiDoi= qd.ID
		group by qd.ID_HangHoa, tblQD.ID_LoHang, tblQD.ID_DonVi
	
				if @CoPhatSinh= 2
					begin
						select 
							a.TenNhomHang,
							a.TenHangHoa,
							a.MaHangHoa,
							a.TenDonViTinh,
							a.TenLoHang,
							a.TenDonVi,
							a.MaDonVi,
							a.TenHangHoaFull,
							---- dauky
							isnull(a.TonDauKy,0) as TonDauKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0),0) as GiaTriDauKy,

							----- trongky
							isnull(a.SoLuongNhap_NCC,0) as SoLuongNhap_NCC,
							isnull(a.SoLuongNhap_Kiem,0) as SoLuongNhap_Kiem,
							isnull(a.SoLuongNhap_Tra,0) as SoLuongNhap_Tra,
							isnull(a.SoLuongNhap_Chuyen,0) as SoLuongNhap_Chuyen,

							isnull(a.SoLuongXuat_Ban,0) as SoLuongXuat_Ban,
							isnull(a.SoLuongXuat_Huy,0) as SoLuongXuat_Huy,
							isnull(a.SoLuongXuat_NCC,0) as SoLuongXuat_NCC,
							isnull(a.SoLuongXuat_Kiem,0) as SoLuongXuat_Kiem,
							isnull(a.SoLuongXuat_Chuyen,0) as SoLuongXuat_Chuyen,

							--cuoiky
							isnull(a.TonDauKy,0) + ( a.SoLuongNhap_NCC + a.SoLuongNhap_Kiem + SoLuongNhap_Tra + SoLuongNhap_Chuyen)
								- (SoLuongXuat_Ban + SoLuongXuat_Huy + SoLuongXuat_NCC + SoLuongXuat_Kiem + SoLuongXuat_Chuyen) as TonCuoiKy,
							iif(@XemGiaVon='1',isnull(a.GiaTriDauKy,0) + isnull(a.GiaTri,0),0) as GiaTriCuoiKy

						from
						(
						select 
								isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
								hh.TenHangHoa,
								qd.MaHangHoa,
								concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
								isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
								qd.TenDonViTinh,
								isnull(lo.MaLoHang,'') as TenLoHang,
								dv.TenDonVi,
								dv.MaDonVi,
				
								-- dauky											
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) as TonDauKy,		
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.TonKho, 0) * iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaTriDauKy,
								iif(tkDauKy.ID_DonVi = dv.ID, tkDauKy.GiaVon, 0) as GiaVon,	

									----trongky
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_NCC, 0) as SoLuongNhap_NCC,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Kiem, 0) as SoLuongNhap_Kiem,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Tra, 0) as SoLuongNhap_Tra,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongNhap_Chuyen, 0) as SoLuongNhap_Chuyen,

								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Ban, 0) as SoLuongXuat_Ban,		
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Huy, 0) as SoLuongXuat_Huy,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_NCC, 0) as SoLuongXuat_NCC,	
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Kiem, 0) as SoLuongXuat_Kiem,
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.SoLuongXuat_Chuyen, 0) as SoLuongXuat_Chuyen,
								iif(tkTrongKy.ID_DonVi = dv.ID, tkTrongKy.GiaTri, 0) as GiaTri


							from #temp tkTrongKy
							left join DM_HangHoa hh on tkTrongKy.ID_HangHoa= hh.ID
							left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
							left join DonViQuiDoi qd on tkTrongKy.ID_HangHoa = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
							left join DM_LoHang lo on tkTrongKy.ID_LoHang= lo.ID or lo.ID is null
							left join @tkDauKy tkDauKy 
							on hh.ID = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= tkTrongKy.ID_DonVi and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
							cross join DM_DonVi  dv									
							where hh.LaHangHoa= 1
								AND hh.TheoDoi LIKE @TheoDoi	
								and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
								and exists (select ID from @tblChiNhanh cn where dv.ID= cn.id)
									AND ((select count(Name) from @tblSearchString b where 
    								hh.TenHangHoa_KhongDau like N'%'+b.Name+'%' 
    								or hh.TenHangHoa_KyTuDau like N'%'+b.Name+'%' 
    									or hh.TenHangHoa like N'%'+b.Name+'%'
    									or lo.MaLoHang like N'%' +b.Name +'%' 
    									or qd.MaHangHoa like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KhongDau like N'%'+b.Name+'%'
    									or nhom.TenNhomHangHoa_KyTuDau like N'%'+b.Name+'%'
    									or qd.TenDonViTinh like N'%'+b.Name+'%'
    									or qd.ThuocTinhGiaTri like N'%'+b.Name+'%'
										or dv.MaDonVi like N'%'+b.Name+'%'
										or dv.TenDonVi like N'%'+b.Name+'%')=@count or @count=0)		
						) a	order by TenHangHoa, TenDonVi,TenLoHang
	end
				else
				begin
				select 	
							dv.MaDonVi, dv.TenDonVi,
							qd.MaHangHoa,
							hh.TenHangHoa,
							hh.QuyCach,
							lo.ID as ID_LoHang,
							qd.ID as ID_DonViQuyDoi,
							concat(hh.TenHangHoa,ThuocTinhGiaTri) as TenHangHoaFull,
							isnull(ThuocTinhGiaTri,'') as ThuocTinh_GiaTri,
							isnull(qd.TenDonViTinh,'') as TenDonViTinh,
							isnull(lo.MaLoHang,'') as TenLoHang,

							---- dauky
							isnull(tkDauKy.TonKho,0) as TonDauKy,
							isnull(tkDauKy.GiaVon,0) as GiaVon,
							iif(@XemGiaVon ='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0),0)  as GiaTriDauKy,			
							isnull(nhom.TenNhomHangHoa,N'Nhóm Hàng Hóa Mặc Định') TenNhomHang,

							---- trongky
							isnull(SoLuongNhap_NCC,0) as SoLuongNhap_NCC,
							isnull(SoLuongNhap_Kiem,0) as SoLuongNhap_Kiem,
							isnull(SoLuongNhap_Tra,0) as SoLuongNhap_Tra,
							isnull(SoLuongNhap_Chuyen,0) as SoLuongNhap_Chuyen,

							isnull(SoLuongXuat_Ban,0) as SoLuongXuat_Ban,
							isnull(SoLuongXuat_Huy,0) as SoLuongXuat_Huy,
							isnull(SoLuongXuat_NCC,0) as SoLuongXuat_NCC,
							isnull(SoLuongXuat_Kiem,0) as SoLuongXuat_Kiem,
							isnull(SoLuongXuat_Chuyen,0) as SoLuongXuat_Chuyen,

							---- cuoiky
							isnull(tkDauKy.TonKho,0) + isnull(SoLuongNhap_NCC,0) +isnull(SoLuongNhap_Kiem,0) +  isnull(SoLuongNhap_Tra,0) + isnull(SoLuongNhap_Chuyen,0)
							- (isnull(SoLuongXuat_Ban,0) + isnull(SoLuongXuat_Huy,0) + isnull(SoLuongXuat_NCC,0) + isnull(SoLuongXuat_Kiem,0) +  isnull(SoLuongXuat_Chuyen,0)) as TonCuoiKy,
							iif(@XemGiaVon ='1',isnull(tkDauKy.TonKho,0) * isnull(tkDauKy.GiaVon,0) + isnull(tkTrongKy.GiaTri,0),0)  as GiaTriCuoiKy
					from DM_HangHoa hh 		
					join DonViQuiDoi qd on hh.ID = qd.ID_HangHoa and qd.LaDonViChuan='1' and qd.Xoa like @TrangThai
					left join DM_LoHang lo on hh.ID = lo.ID_HangHoa
					left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
					cross join DM_DonVi  dv		
					left join @tkDauKy tkDauKy 
					on qd.ID_HangHoa = tkDauKy.ID_HangHoa and tkDauKy.ID_DonVi= dv.ID and ((lo.ID= tkDauKy.ID_LoHang) or (lo.ID is null ))
					left join #temp tkTrongKy on qd.ID_HangHoa = tkTrongKy.ID_HangHoa and tkTrongKy.ID_DonVi= dv.ID and ((lo.ID= tkTrongKy.ID_LoHang) or (lo.ID is null ))
					where hh.LaHangHoa= 1
					AND hh.TheoDoi LIKE @TheoDoi	
					and exists (SELECT ID FROM dbo.GetListNhomHangHoa(@ID_NhomHang) allnhh where nhom.ID = allnhh.ID)	
					and exists (select ID from @tblChiNhanh cn where dv.ID= cn.id)
						AND ((select count(Name) from @tblSearchString b where 
    					hh.TenHangHoa_KhongDau like N'%'+b.Name+'%' 
    					or hh.TenHangHoa_KyTuDau like N'%'+b.Name+'%' 
    						or hh.TenHangHoa like N'%'+b.Name+'%'
    						or lo.MaLoHang like N'%' +b.Name +'%' 
    						or qd.MaHangHoa like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KhongDau like N'%'+b.Name+'%'
    						or nhom.TenNhomHangHoa_KyTuDau like N'%'+b.Name+'%'
    						or qd.TenDonViTinh like N'%'+b.Name+'%'
    						or qd.ThuocTinhGiaTri like N'%'+b.Name+'%'
							or dv.MaDonVi like N'%'+b.Name+'%'
							or dv.TenDonVi like N'%'+b.Name+'%')=@count or @count=0)		
						order by TenHangHoa, TenDonVi,MaLoHang
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
				isnull(hd.PhaiThanhToanBaoHiem,0) as  PhaiThanhToanBaoHiem
			FROM
			(
					select 
						hd.ID			
					from
					(				
						select 
							cthd.ID,
							sum(cthd.SoLuongBan - isnull(cthd.SoLuongTra,0) - isnull(cthd.SoLuongDung,0)) as SoLuongConLai
						from
						(
									------ mua ----
										select 
											ct.ID,
											ct.SoLuong as SoLuongBan,
											0 as SoLuongTra,
											0 as SoLuongDung
										from BH_HoaDon hd
										join BH_HoaDon_ChiTiet ct on hd.ID= ct.ID_HoaDon
										where hd.ChoThanhToan=0
										and hd.LoaiHoaDon = 19 ---- chi lay GDV (vi tra HD dung sp rieng)
										and hd.NgayLapHoaDon between @DateFrom and @DateTo	
										and (@IDChiNhanhs ='' or exists (select ID from @tblChiNhanh cn where hd.ID_DonVi = cn.ID))
										and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID) ---- chi get hanghoa + dv
										and (ct.ID_ParentCombo is null OR ct.ID_ParentCombo != ct.ID) ---- khong get parent, get TP combo
						

										union all

										----- tra ----
										select ct.ID_ChiTietGoiDV,
											0 as SoLuongBan,
											ct.SoLuong as SoLuongTra,
											0 as SoLuongDung
										from BH_HoaDon hd
										join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon  
										where hd.ChoThanhToan = 0  
										and hd.LoaiHoaDon = 6
										and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID)													
							

										union all
										----- sudung ----
											select ct.ID_ChiTietGoiDV,
											0 as SoLuongBan,
											0 as SoLuongTra,
											ct.SoLuong as SoLuongDung
										from BH_HoaDon hd
										join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon  
										where hd.ChoThanhToan = 0  
										and hd.LoaiHoaDon = 1
										and (ct.ID_ChiTietDinhLuong is null OR ct.ID_ChiTietDinhLuong = ct.ID)													
							) cthd
							group by cthd.ID
					)cthConLai
					join BH_HoaDon_ChiTiet ct on cthConLai.ID=  ct.ID
					join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
					where cthConLai.SoLuongConLai > 0
					group by hd.ID
			) tblConLai 
			JOIN BH_HoaDon hd ON tblConLai.ID =	hd.ID	
			left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID 
			left join Gara_DanhMucXe xe on hd.ID_Xe = xe.ID		
			where ((select count(Name) from @tblSearch b where     			
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

			Sql(@"ALTER PROCEDURE [dbo].[GetTonKho_byIDQuyDois]
    @ID_ChiNhanh [uniqueidentifier],
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

			Sql(@"ALTER PROCEDURE [dbo].[GetListBaoHiem_v1]
    @IdChiNhanhs [nvarchar](max),
    @NgayTaoFrom [datetime],
    @NgayTaoTo [datetime],
    @TongBanDateFrom [datetime],
    @TongBanDateTo [datetime],
    @TongBanFrom [float],
    @TongBanTo [float],
    @NoFrom [float],
    @NoTo [float],
    @TrangThais [nvarchar](20),
    @TextSearch [nvarchar](max),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    
    	declare @tblDonVi table (ID_DonVi  uniqueidentifier);
    	if(@IdChiNhanhs != '')
    	BEGIN
    		insert into @tblDonVi
    		select Name from dbo.splitstring(@IdChiNhanhs);
    	END
    
    	DECLARE @tblSearch TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearch(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearch);
    
    	declare @tbTrangThai table (GiaTri varchar(2));
    	if(@TrangThais != '')
    	BEGIN
    		insert into @tbTrangThai
    		select Name from dbo.splitstring(@TrangThais);
    	END
    	DECLARE @tblResult TABLE(ID UNIQUEIDENTIFIER, MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), DienThoai NVARCHAR(MAX), MaSoThue NVARCHAR(MAX), Email NVARCHAR(MAX), DiaChi NVARCHAR(MAX), ID_TinhThanh UNIQUEIDENTIFIER, 
    	TenTinhThanh NVARCHAR(MAX), ID_QuanHuyen UNIQUEIDENTIFIER, TenQuanHuyen NVARCHAR(MAX),
    	GhiChu NVARCHAR(MAX), ID_DonVi UNIQUEIDENTIFIER, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), NgayTao DATETIME, LoaiDoiTuong INT, NguoiTao NVARCHAR(MAX), NoHienTai FLOAT, TongTienBaoHiem FLOAT, TotalRow INT, TotalPage FLOAT);
    
    
    	DECLARE @tblDoiTuong TABLE(ID UNIQUEIDENTIFIER, MaDoiTuong NVARCHAR(MAX), TenDoiTuong NVARCHAR(MAX), DienThoai NVARCHAR(MAX), MaSoThue NVARCHAR(MAX), Email NVARCHAR(MAX), DiaChi NVARCHAR(MAX), ID_TinhThanh UNIQUEIDENTIFIER, 
    	TenTinhThanh NVARCHAR(MAX), ID_QuanHuyen UNIQUEIDENTIFIER, TenQuanHuyen NVARCHAR(MAX),
    	GhiChu NVARCHAR(MAX), ID_DonVi UNIQUEIDENTIFIER, MaDonVi NVARCHAR(MAX), TenDonVi NVARCHAR(MAX), NgayTao DATETIME, LoaiDoiTuong INT, NguoiTao NVARCHAR(MAX));
    	DECLARE @tblBaoHiemPhaiThanhToan TABLE(ID UNIQUEIDENTIFIER, PhaiThanhToan FLOAT);
    	DECLARE @tblBaoHiemDaThanhToan TABLE(ID UNIQUEIDENTIFIER, DaThanhToan FLOAT);
    	DECLARE @TotalRow INT;
    	DECLARE @TotalPage FLOAT;
    
    	IF (@TongBanFrom IS NULL AND @TongBanTo IS NULL AND @NoFrom IS NULL AND @NoTo IS NULL)
    	BEGIN
    		
    		INSERT INTO @tblDoiTuong
    		SELECT dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.MaSoThue, dt.Email, dt.DiaChi, dt.ID_TinhThanh, tt.TenTinhThanh, dt.ID_QuanHuyen, qh.TenQuanHuyen,
    		dt.GhiChu, dt.ID_DonVi, dv.MaDonVi, dv.TenDonVi, dt.NgayTao, dt.LoaiDoiTuong, dt.NguoiTao FROM DM_DoiTuong dt
    		LEFT JOIN DM_TinhThanh tt ON dt.ID_TinhThanh = tt.ID
    		LEFT JOIN DM_QuanHuyen qh ON dt.ID_QuanHuyen = qh.ID
    		inner join DM_DonVi dv on dv.ID = dt.ID_DonVi
    		inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    		INNER JOIN @tbTrangThai tth ON dt.TheoDoi = tth.GiaTri
    		WHERE 
    			dt.LoaiDoiTuong  = 3
    			AND (@NgayTaoFrom IS NULL OR dt.NgayTao BETWEEN @NgayTaoFrom AND @NgayTaoTo)
    			AND ((select count(Name) from @tblSearch b where     			
    			dt.GhiChu like '%'+b.Name+'%'
    			or dt.MaDoiTuong like '%'+b.Name+'%'		
    			or dt.TenDoiTuong like '%'+b.Name+'%'
    			or dt.DienThoai like '%'+b.Name+'%'
    			or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    			or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    			or dt.MaSoThue like '%'+b.Name+'%'
    			or dt.DiaChi like '%'+b.Name+'%'
    			or dt.Email like '%'+b.Name+'%'
    			or tt.TenTinhThanh like '%'+b.Name+'%'
    			or qh.TenQuanHuyen like '%'+b.Name+'%'
    			or dv.MaDonVi like '%'+b.Name+'%'
    			or dv.TenDonVi like '%'+b.Name+'%'
    			)=@count or @count=0);
    
    			--SELECT * FROM @tblDoiTuong;
				IF (@PageSize != 0)
				BEGIN
    				INSERT INTO @tblBaoHiemPhaiThanhToan
    				select hd.ID_BaoHiem, SUM(hd.PhaiThanhToanBaoHiem) AS TongBaoHiem from BH_HoaDon hd
    				INNER JOIN (SELECT * FROM @tblDoiTuong dtt ORDER BY dtt.NgayTao desc
    							OFFSET (@CurrentPage * @PageSize) ROWS
    							FETCH NEXT @PageSize ROWS ONLY) dt ON dt.ID = hd.ID_BaoHiem 
    				inner join @tblDonVi donvi on donvi.ID_DonVi = hd.ID_DonVi
    				where hd.LoaiHoaDon = 25 and ChoThanhToan = 0
    				AND ID_BaoHiem IS NOT NULL
    				AND (@TongBanDateFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo)
    				GROUP BY ID_BaoHiem
    				--HAVING (@TongBanFrom IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanFrom)
    				--AND (@TongBanTo IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanTo)
    		
    				INSERT INTO @tblBaoHiemDaThanhToan
    				SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    				INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				INNER JOIN (SELECT * FROM @tblDoiTuong dtt ORDER BY dtt.NgayTao desc
    							OFFSET (@CurrentPage * @PageSize) ROWS
    							FETCH NEXT @PageSize ROWS ONLY) dt ON dt.ID = qhdct.ID_DoiTuong
    				inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    				WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) AND qhdct.HinhThucThanhToan != 6
					AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    				GROUP BY qhdct.ID_DoiTuong
    				--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    				--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    				SELECT @TotalRow = COUNT(ID), @TotalPage = CEILING(COUNT(ID) / CAST(@PageSize as float )) FROM @tblDoiTuong;
    				INSERT INTO @tblResult
    				SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    				LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    				LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    				ORDER BY dt.NgayTao desc
    				OFFSET (@CurrentPage * @PageSize) ROWS
    				FETCH NEXT @PageSize ROWS ONLY;
				END
				ELSE
				BEGIN
					INSERT INTO @tblBaoHiemPhaiThanhToan
    				select hd.ID_BaoHiem, SUM(hd.PhaiThanhToanBaoHiem) AS TongBaoHiem from BH_HoaDon hd
    				INNER JOIN @tblDoiTuong dt ON dt.ID = hd.ID_BaoHiem 
    				inner join @tblDonVi donvi on donvi.ID_DonVi = hd.ID_DonVi
    				where hd.LoaiHoaDon = 25 and ChoThanhToan = 0
    				AND ID_BaoHiem IS NOT NULL
    				AND (@TongBanDateFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo)
    				GROUP BY ID_BaoHiem
    				--HAVING (@TongBanFrom IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanFrom)
    				--AND (@TongBanTo IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanTo)
    		
    				INSERT INTO @tblBaoHiemDaThanhToan
    				SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    				INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    				INNER JOIN @tblDoiTuong dt ON dt.ID = qhdct.ID_DoiTuong
    				inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    				WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) AND qhdct.HinhThucThanhToan != 6
					AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    				GROUP BY qhdct.ID_DoiTuong
    				--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    				--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    				SELECT @TotalRow = 0, @TotalPage = 0 FROM @tblDoiTuong;
    				INSERT INTO @tblResult
    				SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    				LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    				LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    				ORDER BY dt.NgayTao desc;
				END
    	END
    	ELSE
    	BEGIN
    		IF(@NoFrom IS NULL AND @NoTo IS NULL)
    		BEGIN
    			IF(@TongBanFrom = 0 OR @TongBanTo = 0 OR @TongBanFrom IS NULL)
    			BEGIN
    				INSERT INTO @tblBaoHiemPhaiThanhToan
    				select hd.ID_BaoHiem, SUM(hd.PhaiThanhToanBaoHiem) AS TongBaoHiem from BH_HoaDon hd
    				inner join @tblDonVi donvi on donvi.ID_DonVi = hd.ID_DonVi
    				where hd.LoaiHoaDon = 25 and ChoThanhToan = 0
    				AND ID_BaoHiem IS NOT NULL
    				AND (@TongBanDateFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo)
    				GROUP BY ID_BaoHiem
    				HAVING (@TongBanFrom IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanFrom)
    				AND (@TongBanTo IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) <= @TongBanTo)
    
    				INSERT INTO @tblDoiTuong
    				SELECT dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.MaSoThue, dt.Email, dt.DiaChi, dt.ID_TinhThanh, tt.TenTinhThanh, dt.ID_QuanHuyen, qh.TenQuanHuyen,
    				dt.GhiChu, dt.ID_DonVi, dv.MaDonVi, dv.TenDonVi, dt.NgayTao, dt.LoaiDoiTuong, dt.NguoiTao FROM DM_DoiTuong dt
    				LEFT JOIN DM_TinhThanh tt ON dt.ID_TinhThanh = tt.ID
    				LEFT JOIN DM_QuanHuyen qh ON dt.ID_QuanHuyen = qh.ID
    				inner join DM_DonVi dv on dv.ID = dt.ID_DonVi
    				inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    				INNER JOIN @tbTrangThai tth ON dt.TheoDoi = tth.GiaTri
    				LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    				WHERE 
    					dt.LoaiDoiTuong  = 3 AND (ISNULL(ptt.PhaiThanhToan, 0) >= @TongBanFrom OR @TongBanFrom IS NULL) AND (ISNULL(ptt.PhaiThanhToan, 0) <= @TongBanTo OR @TongBanTo IS NULL)
    					AND (@NgayTaoFrom IS NULL OR dt.NgayTao BETWEEN @NgayTaoFrom AND @NgayTaoTo)
    					AND ((select count(Name) from @tblSearch b where     			
    					dt.GhiChu like '%'+b.Name+'%'
    					or dt.MaDoiTuong like '%'+b.Name+'%'		
    					or dt.TenDoiTuong like '%'+b.Name+'%'
    					or dt.DienThoai like '%'+b.Name+'%'
    					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    					or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    					or dt.MaSoThue like '%'+b.Name+'%'
    					or dt.DiaChi like '%'+b.Name+'%'
    					or dt.Email like '%'+b.Name+'%'
    					or tt.TenTinhThanh like '%'+b.Name+'%'
    					or qh.TenQuanHuyen like '%'+b.Name+'%'
    					or dv.MaDonVi like '%'+b.Name+'%'
    					or dv.TenDonVi like '%'+b.Name+'%'
    					)=@count or @count=0);
					IF (@PageSize != 0)
					BEGIN
    					INSERT INTO @tblBaoHiemDaThanhToan
    					SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    					INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    					INNER JOIN (SELECT * FROM @tblDoiTuong dtt ORDER BY dtt.NgayTao desc
    								OFFSET (@CurrentPage * @PageSize) ROWS
    								FETCH NEXT @PageSize ROWS ONLY) dt ON dt.ID = qhdct.ID_DoiTuong
    					inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    					WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) AND qhdct.HinhThucThanhToan != 6
						AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    					GROUP BY qhdct.ID_DoiTuong
    					--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    					--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    					SELECT @TotalRow = COUNT(ID), @TotalPage = CEILING(COUNT(ID) / CAST(@PageSize as float )) FROM @tblDoiTuong;
    					INSERT INTO @tblResult
    					SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    					LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    					LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    					ORDER BY dt.NgayTao desc
    					OFFSET (@CurrentPage * @PageSize) ROWS
    					FETCH NEXT @PageSize ROWS ONLY;
					END
					ELSE
					BEGIN
						INSERT INTO @tblBaoHiemDaThanhToan
    					SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    					INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    					INNER JOIN @tblDoiTuong dt ON dt.ID = qhdct.ID_DoiTuong
    					inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    					WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) AND qhdct.HinhThucThanhToan != 6
						AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    					GROUP BY qhdct.ID_DoiTuong
    					--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    					--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    					SELECT @TotalRow = 0, @TotalPage = 0 FROM @tblDoiTuong;
    					INSERT INTO @tblResult
    					SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    					LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    					LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    					ORDER BY dt.NgayTao desc;
					END
    			END
    			ELSE
    			BEGIN
    				INSERT INTO @tblBaoHiemPhaiThanhToan
    				select hd.ID_BaoHiem, SUM(hd.PhaiThanhToanBaoHiem) AS TongBaoHiem from BH_HoaDon hd
    				inner join @tblDonVi donvi on donvi.ID_DonVi = hd.ID_DonVi
    				where hd.LoaiHoaDon = 25 and ChoThanhToan = 0
    				AND ID_BaoHiem IS NOT NULL
    				AND (@TongBanDateFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo)
    				GROUP BY ID_BaoHiem
    				HAVING (@TongBanFrom IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanFrom)
    				AND (@TongBanTo IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) <= @TongBanTo)
    
    				INSERT INTO @tblDoiTuong
    				SELECT dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.MaSoThue, dt.Email, dt.DiaChi, dt.ID_TinhThanh, tt.TenTinhThanh, dt.ID_QuanHuyen, qh.TenQuanHuyen,
    				dt.GhiChu, dt.ID_DonVi, dv.MaDonVi, dv.TenDonVi, dt.NgayTao, dt.LoaiDoiTuong, dt.NguoiTao FROM DM_DoiTuong dt
    				LEFT JOIN DM_TinhThanh tt ON dt.ID_TinhThanh = tt.ID
    				LEFT JOIN DM_QuanHuyen qh ON dt.ID_QuanHuyen = qh.ID
    				inner join DM_DonVi dv on dv.ID = dt.ID_DonVi
    				inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    				INNER JOIN @tbTrangThai tth ON dt.TheoDoi = tth.GiaTri
    				INNER JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    				WHERE 
    					dt.LoaiDoiTuong  = 3
    					AND (@NgayTaoFrom IS NULL OR dt.NgayTao BETWEEN @NgayTaoFrom AND @NgayTaoTo)
    					AND ((select count(Name) from @tblSearch b where     			
    					dt.GhiChu like '%'+b.Name+'%'
    					or dt.MaDoiTuong like '%'+b.Name+'%'		
    					or dt.TenDoiTuong like '%'+b.Name+'%'
    					or dt.DienThoai like '%'+b.Name+'%'
    					or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    					or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    					or dt.MaSoThue like '%'+b.Name+'%'
    					or dt.DiaChi like '%'+b.Name+'%'
    					or dt.Email like '%'+b.Name+'%'
    					or tt.TenTinhThanh like '%'+b.Name+'%'
    					or qh.TenQuanHuyen like '%'+b.Name+'%'
    					or dv.MaDonVi like '%'+b.Name+'%'
    					or dv.TenDonVi like '%'+b.Name+'%'
    					)=@count or @count=0);
						IF (@PageSize != 0)
						BEGIN
    						INSERT INTO @tblBaoHiemDaThanhToan
    						SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    						INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    						INNER JOIN (SELECT * FROM @tblDoiTuong dtt ORDER BY dtt.NgayTao desc
    									OFFSET (@CurrentPage * @PageSize) ROWS
    									FETCH NEXT @PageSize ROWS ONLY) dt ON dt.ID = qhdct.ID_DoiTuong
    						inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    						WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) and qhdct.HinhThucThanhToan != 6
							AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    						GROUP BY qhdct.ID_DoiTuong
    						--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    						--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    						SELECT @TotalRow = COUNT(ID), @TotalPage = CEILING(COUNT(ID) / CAST(@PageSize as float )) FROM @tblDoiTuong;
    						INSERT INTO @tblResult
    						SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    						LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    						LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    						ORDER BY dt.NgayTao desc
    						OFFSET (@CurrentPage * @PageSize) ROWS
    						FETCH NEXT @PageSize ROWS ONLY;
						END
						ELSE
						BEGIN
							INSERT INTO @tblBaoHiemDaThanhToan
    						SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    						INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    						INNER JOIN @tblDoiTuong dt ON dt.ID = qhdct.ID_DoiTuong
    						inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    						WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) and qhdct.HinhThucThanhToan != 6
							AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    						GROUP BY qhdct.ID_DoiTuong
    						--HAVING (@TongBanFrom IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanFrom)
    						--AND (@TongBanTo IS NULL OR SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) >= @TongBanTo)
    						SELECT @TotalRow = 0, @TotalPage = 0 FROM @tblDoiTuong;
    						INSERT INTO @tblResult
    						SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    						LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    						LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    						ORDER BY dt.NgayTao desc;
						END
    				END
    		END
    		ELSE
    		BEGIN
    			INSERT INTO @tblBaoHiemPhaiThanhToan
    			select hd.ID_BaoHiem, SUM(hd.PhaiThanhToanBaoHiem) AS TongBaoHiem from BH_HoaDon hd
    			inner join @tblDonVi donvi on donvi.ID_DonVi = hd.ID_DonVi
    			where hd.LoaiHoaDon = 25 and ChoThanhToan = 0
    			AND ID_BaoHiem IS NOT NULL
    			AND (@TongBanDateFrom IS NULL OR hd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo)
    			GROUP BY ID_BaoHiem
    			HAVING (@TongBanFrom IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) >= @TongBanFrom)
    			AND (@TongBanTo IS NULL OR SUM(hd.PhaiThanhToanBaoHiem) <= @TongBanTo);
    
    			INSERT INTO @tblBaoHiemDaThanhToan
    			SELECT qhdct.ID_DoiTuong, SUM(CASE WHEN qhd.LoaiHoaDon = 11 THEN qhdct.TienThu ELSE -1 * qhdct.TienThu END) AS DaThanhToan FROM Quy_HoaDon qhd
    			INNER JOIN Quy_HoaDon_ChiTiet qhdct ON qhd.ID = qhdct.ID_HoaDon
    			inner join @tblDonVi donvi on donvi.ID_DonVi = qhd.ID_DonVi
    			WHERE (@TongBanDateFrom IS NULL OR qhd.NgayLapHoaDon BETWEEN @TongBanDateFrom AND @TongBanDateTo) and qhdct.HinhThucThanhToan != 6
				AND qhd.TrangThai = 1 and qhd.PhieuDieuChinhCongNo != 3
    			GROUP BY qhdct.ID_DoiTuong;
    
    			INSERT INTO @tblDoiTuong
    			SELECT dt.ID, dt.MaDoiTuong, dt.TenDoiTuong, dt.DienThoai, dt.MaSoThue, dt.Email, dt.DiaChi, dt.ID_TinhThanh, tt.TenTinhThanh, dt.ID_QuanHuyen, qh.TenQuanHuyen,
    			dt.GhiChu, dt.ID_DonVi, dv.MaDonVi, dv.TenDonVi, dt.NgayTao, dt.LoaiDoiTuong, dt.NguoiTao FROM DM_DoiTuong dt
    			LEFT JOIN DM_TinhThanh tt ON dt.ID_TinhThanh = tt.ID
    			LEFT JOIN DM_QuanHuyen qh ON dt.ID_QuanHuyen = qh.ID
    			inner join DM_DonVi dv on dv.ID = dt.ID_DonVi
    			inner join @tblDonVi donvi on donvi.ID_DonVi = dv.ID
    			INNER JOIN @tbTrangThai tth ON dt.TheoDoi = tth.GiaTri
    			LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    			LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    			WHERE 
    				dt.LoaiDoiTuong  = 3
    				AND (ISNULL(ptt.PhaiThanhToan, 0) >= @TongBanFrom OR @TongBanFrom IS NULL) AND (ISNULL(ptt.PhaiThanhToan, 0) <= @TongBanTo OR @TongBanTo IS NULL)
    				AND ((ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0)) >= @NoFrom OR @NoFrom IS NULL) AND ((ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0)) <= @NoTo OR @NoTo IS NULL)
    				AND (@NgayTaoFrom IS NULL OR dt.NgayTao BETWEEN @NgayTaoFrom AND @NgayTaoTo)
    				AND ((select count(Name) from @tblSearch b where     			
    				dt.GhiChu like '%'+b.Name+'%'
    				or dt.MaDoiTuong like '%'+b.Name+'%'		
    				or dt.TenDoiTuong like '%'+b.Name+'%'
    				or dt.DienThoai like '%'+b.Name+'%'
    				or dt.TenDoiTuong_KhongDau like '%'+b.Name+'%'
    				or dt.TenDoiTuong_ChuCaiDau like '%'+b.Name+'%'
    				or dt.MaSoThue like '%'+b.Name+'%'
    				or dt.DiaChi like '%'+b.Name+'%'
    				or dt.Email like '%'+b.Name+'%'
    				or tt.TenTinhThanh like '%'+b.Name+'%'
    				or qh.TenQuanHuyen like '%'+b.Name+'%'
    				or dv.MaDonVi like '%'+b.Name+'%'
    				or dv.TenDonVi like '%'+b.Name+'%'
    				)=@count or @count=0);
					IF(@PageSize != 0)
					BEGIN
    					SELECT @TotalRow = COUNT(ID), @TotalPage = CEILING(COUNT(ID) / CAST(@PageSize as float )) FROM @tblDoiTuong;
    					INSERT INTO @tblResult
    					SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    					LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    					LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    					ORDER BY dt.NgayTao desc
    					OFFSET (@CurrentPage * @PageSize) ROWS
    					FETCH NEXT @PageSize ROWS ONLY;
					END
					ELSE
					BEGIN
						SELECT @TotalRow = 0, @TotalPage = 0 FROM @tblDoiTuong;
    					INSERT INTO @tblResult
    					SELECT dt.*, ISNULL(ptt.PhaiThanhToan, 0) - ISNULL(dtt.DaThanhToan, 0) AS NoHienTai, ISNULL(ptt.PhaiThanhToan, 0) AS TongTienBaoHiem, @TotalRow AS TotalRow, @TotalPage AS TotalPage FROM @tblDoiTuong dt
    					LEFT JOIN @tblBaoHiemPhaiThanhToan ptt ON ptt.ID = dt.ID
    					LEFT JOIN @tblBaoHiemDaThanhToan dtt ON dtt.ID = dt.ID
    					ORDER BY dt.NgayTao desc;
					END
    		END
    	END
    		
    		SELECT ID, MaDoiTuong, TenDoiTuong , DienThoai , MaSoThue , Email , DiaChi , ID_TinhThanh , 
    	TenTinhThanh , ID_QuanHuyen , TenQuanHuyen ,
    	GhiChu , ID_DonVi , MaDonVi , TenDonVi , NgayTao , LoaiDoiTuong , NguoiTao , ROUND(NoHienTai, 0) AS NoHienTai , ROUND(TongTienBaoHiem,0) AS TongTienBaoHiem, TotalRow , TotalPage  FROM @tblResult
		ORDER BY NgayTao desc;
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
								 and hd.LoaiHoaDon in (22)
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
						ctm.ThanhTien,
						ctm.ThanhTien  * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
						isnull(tbl.SoLuongSuDung,0) * (ctm.DonGia - ctm.TienChietKhau) * ( 1 - ctm.PTGiamGiaHD)  as GiaTriSD,
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
						ctm.ThanhTien,
						ctm.PTGiamGiaHD * ctm.ThanhTien as GiamGiaHD,
						ctm.ThanhTien  * ( 1 -ctm.PTGiamGiaHD) as GiaTriMua,
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
						isnull(tbl.SoLuongSuDung,0) * (ctm.DonGia - ctm.TienChietKhau) * ( 1 - ctm.PTGiamGiaHD)  as GiaTriSD,
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
					sum(iif(hd.LoaiHoaDon = 6,iif(hd.TongTienHang=0, 0, ct.ThanhTien * (1- hd.TongGiamGia/hd.TongTienHang)) ,0)) as GiaTriTra

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
						iif(ctkm.ID_TangKem is not null, ctkm.SoLuong, ct.SoLuong) - isnull(SoLuongTra,0) as SoLuong,
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

			Sql(@"ALTER PROCEDURE [dbo].[XoaDuLieuHeThong]
    @CheckHH [int],
    @CheckKH [int]
AS
BEGIN
SET NOCOUNT ON;

				delete from chotso
				delete from BH_HoaDon_ChiPhi
				delete from DM_MauIn
				delete from NS_CongViec
				delete from NS_CongViec_PhanLoai
    			delete from chotso_hanghoa
    			delete from chotso_khachHang
				delete from BH_NhanVienThucHien
    			delete from Quy_HoaDon_ChiTiet
    			delete from Quy_KhoanThuChi
    			delete from Quy_HoaDon
				delete from DM_TaiKhoanNganHang    			
    			delete from BH_HoaDon_ChiTiet
    			delete from BH_HoaDon
    			delete from DM_GiaBan_ApDung
    			delete from DM_GiaBan_ChiTiet
    			delete from DM_GiaBan
    			delete from ChamSocKhachHangs
				delete from HeThong_SMS
				delete from HeThong_SMS_TaiKhoan
				delete from HeThong_SMS_TinMau
				delete from ChietKhauMacDinh_NhanVien
				delete from ChietKhauMacDinh_HoaDon_ChiTiet
				delete from ChietKhauMacDinh_HoaDon
				delete from ChietKhauDoanhThu_NhanVien
				delete from ChietKhauDoanhThu_ChiTiet
				delete from ChietKhauDoanhThu
				delete from NhomDoiTuong_DonVi
				delete from DM_KhuyenMai_ChiTiet
    			delete from DM_KhuyenMai_ApDung
    			delete from DM_KhuyenMai
    			delete from ChietKhauMacDinh_NhanVien   
				 
				delete from Gara_HangMucSuaChua
				delete from Gara_GiayToKemTheo
    			delete from DM_KhuyenMai_ApDung
    			delete from DM_KhuyenMai
    			delete from ChietKhauMacDinh_NhanVien   
				delete from Gara_PhieuTiepNhan
				delete from Gara_DanhMucXe
    			----delete from Gara_MauXe where id not like '%00000000-0000-0000-0000-000000000000%'
    			----delete from Gara_HangXe where id not like '%00000000-0000-0000-0000-000000000000%'
    			----delete from Gara_LoaiXe where id not like '%00000000-0000-0000-0000-000000000000%'
				
    			if(@CheckKH =0)
    			BEGIN
					delete from DM_LienHe_Anh
    				delete from DM_LienHe
					delete from DM_DoiTuong_Anh
					delete from DM_DoiTuong_Nhom
    				delete from DM_DoiTuong WHERE ID != '00000000-0000-0000-0000-000000000002' AND ID != '00000000-0000-0000-0000-000000000000'
					delete from DM_NguonKhachHang
					delete from DM_DoiTuong_TrangThai
    				delete from DM_NhomDoiTuong	  
    									
    			END
    			ELSE 
    			BEGIN
    				UPDATE DM_DoiTuong SET ID_DonVi = 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE', ID_NhanVienPhuTrach = null, TongTichDiem = 0 
    			END
    		 			
    			if(@CheckHH = 0)
    			BEGIN
						delete from DM_GiaVon
						delete from DM_HangHoa_TonKho
    				   	delete from DinhLuongDichVu
    					delete from DonViQuiDoi
    					delete from HangHoa_ThuocTinh
						delete from DM_HangHoa_ViTri  
    					delete from DM_HangHoa_Anh
    					delete from DM_HangHoa  				
    					delete from DM_ThuocTinh				  				  				
    					delete from DM_NhomHangHoa where ID != '00000000-0000-0000-0000-000000000000' and ID != '00000000-0000-0000-0000-000000000001'
    			END
				ELSE
				BEGIN
					DELETE DM_GiaVon WHERE ID_LoHang is not null
					DELETE DM_GiaVon WHERE ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
					DELETE DM_HangHoa_TonKho WHERE ID_LoHang is not null
					DELETE DM_HangHoa_TonKho WHERE ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
					UPDATE DM_GiaVon SET GiaVon = 0
					UPDATE DM_HangHoa_TonKho SET TonKho = 0
				END
				
    			delete from DM_LoHang
    			delete from DM_ViTri
    			delete from DM_KhuVuc
    			
    			delete from HT_NhatKySuDung where LoaiNhatKy != 20 and LoaiNhatKy != 21
    					
    			delete from CongDoan_DichVu
    			delete from CongNoDauKi
    			delete from DanhSachThi_ChiTiet	
    			delete from DanhSachThi
    			delete from DM_ChucVu
    			delete from DM_HinhThucThanhToan
    			delete from DM_HinhThucVanChuyen
    			delete from DM_KhoanPhuCap
    			delete from DM_LoaiGiaPhong
    			delete from DM_LoaiNhapXuat
    			delete from DM_LoaiPhieuThanhToan
    			delete from DM_LoaiPhong
    			delete from DM_LoaiTuVanLichHen
    			delete from DM_LopHoc
    			delete from DM_LyDoHuyLichHen
    			delete from DM_MaVach
    			delete from DM_MayChamCong
    			delete from DM_NoiDungQuanTam
    			delete from DM_PhanLoaiHangHoaDichVu
    			delete from DM_ThueSuat
    			
    			delete from HT_CauHinh_TichDiemApDung
    			delete from HT_CauHinh_TichDiemChiTiet		
    			delete from DM_TichDiem	
    			delete from NhomDoiTuong_DonVi where ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
    			delete from NhomHangHoa_DonVi where ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
    			delete from NS_LuongDoanhThu_ChiTiet 
    			delete from NS_LuongDoanhThu
    			delete from NS_HoSoLuong 
    			delete from The_NhomThe
    			delete from The_TheKhachHang_ChiTiet
    			delete from The_TheKhachHang
    
    			delete from HT_ThongBao
    			delete from HT_ThongBao_CaiDat where ID_NguoiDung != '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' 
    			delete from HT_Quyen_Nhom where ID_NhomNguoiDung IN (select ID From HT_NhomNguoiDung where ID NOT IN (select IDNhomNguoiDung from HT_NguoiDung_Nhom where IDNguoiDung = '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' AND ID_DonVi = 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'))
    			--delete from HT_NguoiDung_Nhom where IDNhomNguoiDung IN (select ID From HT_NhomNguoiDung where ID NOT IN (select IDNhomNguoiDung from HT_NguoiDung_Nhom where IDNguoiDung = '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' AND ID_DonVi = 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'))
				delete from HT_NguoiDung_Nhom where IDNguoiDung != '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' 
    			delete from HT_NhomNguoiDung where ID NOT IN (select IDNhomNguoiDung from HT_NguoiDung_Nhom where IDNguoiDung = '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' AND ID_DonVi = 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE')
    				
    			delete from HT_NguoiDung where ID != '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77' 
				
				delete from NS_PhieuPhanCa_CaLamViec
				delete from NS_PhieuPhanCa_NhanVien
				delete from NS_CaLamViec_DonVi
				delete from NS_ThietLapLuongChiTiet
				delete from NS_CongNoTamUngLuong

				delete from NS_CongBoSung
				delete from NS_BangLuong_ChiTiet
				delete from NS_CaLamViec
				delete from NS_BangLuong			
				delete from NS_KyHieuCong
				delete from NS_NgayNghiLe
				delete from NS_PhieuPhanCa

				delete from NS_MienGiamThue
				delete from NS_KhenThuong
				delete from NS_HopDong
				delete from NS_BaoHiem
				delete from NS_Luong_PhuCap
				delete from NS_LoaiLuong
				delete from NS_NhanVien_CongTac
				delete from NS_NhanVien_DaoTao
				delete from NS_NhanVien_GiaDinh
				delete from NS_NhanVien_SucKhoe
				delete from NS_NhanVien_Anh	
    			delete from NS_QuaTrinhCongTac where ID_NhanVien NOT IN (select ID_NhanVien from HT_NguoiDung where ID = '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77') or ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
				update NS_NhanVien SET ID_NSPhongBan = null
    			delete from NS_NhanVien where ID NOT IN (select ID_NhanVien from HT_NguoiDung where ID = '28FEF5A1-F0F2-4B94-A4AD-081B227F3B77')
    			delete from NS_PhongBan	 where ID_DonVi is not null and ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
    			delete from Kho_DonVi where ID_DonVi != 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE'
    			delete from DM_Kho where ID NOT IN (select ID_Kho from Kho_DonVi where ID_DonVi = 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE')
    			delete from DM_DonVi where ID !='D93B17EA-89B9-4ECF-B242-D03B8CDE71DE';
	
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
    	LEFT JOIN BH_HoaDon xk ON hdsc.ID = xk.ID_HoaDon AND xk.ChoThanhToan = 0
    	LEFT JOIN BH_HoaDon_ChiTiet xkct ON xk.ID = xkct.ID_HoaDon
    	WHERE (xk.LoaiHoaDon = 8 AND xk.ChoThanhToan = 0) OR xk.ID IS NULL
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
        }
        
        public override void Down()
        {
            DropStoredProcedure("[dbo].[CTHD_GetAllDonViTinhOfhangHoa]");
			DropStoredProcedure("[dbo].[CTHD_GetAllNhanVienThucHien]");
        }
    }
}
