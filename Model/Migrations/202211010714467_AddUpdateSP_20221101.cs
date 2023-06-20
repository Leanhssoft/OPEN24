namespace Model.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddUpdateSP_20221101 : DbMigration
    {
        public override void Up()
        {
			Sql(@"create type TblID as table
(
	ID uniqueidentifier not null primary key
)");

            Sql(@"CREATE FUNCTION [dbo].[FN_GetMaHangHoa]
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
	WHERE CHARINDEX(@MaHangHoa,MaHangHoa) > 0 
	and CHARINDEX('Copy',MaHangHoa)= 0 and CHARINDEX('_',MaHangHoa)= 0

	RETURN concat(@MaHangHoa,FORMAT(@Return + 1, 'F0'))

END
");

			Sql(@"ALTER FUNCTION [dbo].[GetIDNhanVien_inPhongBan] (
	@ID_NhanVien UNIQUEIDENTIFIER , 
	@IDDonVi varchar(max), 
	@MaQuyenXemPhongBan varchar(100),
	@MaQuyenXemHeThong varchar(100))
RETURNS
 @tblNhanVien TABLE (ID UNIQUEIDENTIFIER)
AS
BEGIN
	
	DECLARE @tblDonVi TABLE (ID UNIQUEIDENTIFIER);
	insert into @tblDonVi
	select Name from dbo.splitstring(@IDDonVi)

	declare @LaAdmin bit=( select top 1 LaAdmin from HT_NguoiDung where ID_NhanVien = @ID_NhanVien)

	declare @countAll int = (SELECT count(*)
	FROM HT_NguoiDung_Nhom nnd
	JOIN HT_Quyen_Nhom qn on nnd.IDNhomNguoiDung = qn.ID_NhomNguoiDung
	JOIN HT_NguoiDung htnd on nnd.IDNguoiDung= htnd.ID
	where htnd.ID_NhanVien= @ID_NhanVien and qn.MaQuyen= @MaQuyenXemHeThong 
	and  exists (select ID from @tblDonVi dv where nnd.ID_DonVi = dv.ID) )

	-- get list phongban congtac
	DECLARE @tblPhongBan TABLE (ID UNIQUEIDENTIFIER);
	INSERT INTO @tblPhongBan
	select ID_PhongBan
	from NS_QuaTrinhCongTac ct where ID_NhanVien= @ID_NhanVien 
	and exists (select ID from @tblDonVi dv where ct.ID_DonVi = dv.ID)

	DECLARE @tblPhongBanTemp TABLE (ID UNIQUEIDENTIFIER);
	INSERT INTO @tblPhongBanTemp
	select ID_PhongBan from NS_QuaTrinhCongTac ct where ID_NhanVien= @ID_NhanVien 
	and exists (select ID from @tblDonVi dv where ct.ID_DonVi = dv.ID)

	if @LaAdmin ='1' or @countAll > 0
	begin
		-- phongban in hethong
		INSERT INTO @tblPhongBan
		SELECT ID FROM NS_PhongBan pb 
		where (exists (select ID from @tblDonVi dv where pb.ID_DonVi = dv.ID) or ID_DonVi is null); 

		-- get allNVien (lay ca NV khong thuoc chi nhanh)
		INSERT INTO @tblNhanVien
		select distinct ct.ID_NhanVien
		from NS_QuaTrinhCongTac ct

	end
	else	
		begin
			declare @countByPhong int = (SELECT count(*)
			FROM HT_NguoiDung_Nhom nnd
			JOIN HT_Quyen_Nhom qn on nnd.IDNhomNguoiDung = qn.ID_NhomNguoiDung
			JOIN HT_NguoiDung htnd on nnd.IDNguoiDung= htnd.ID
			where htnd.ID_NhanVien= @ID_NhanVien and qn.MaQuyen= @MaQuyenXemPhongBan 
			and  exists (select ID from @tblDonVi dv where nnd.ID_DonVi = dv.ID) )

			if @countByPhong > 0
				begin
					DECLARE @intFlag INT;
					SET @intFlag = 1;
					WHILE (@intFlag != 0)
					BEGIN
						SELECT @intFlag = COUNT(ID) FROM NS_PhongBan pb
						WHERE ID_PhongBanCha IN (SELECT ID FROM @tblPhongBanTemp) 
						IF(@intFlag != 0)
						BEGIN
							-- get phongban con
							INSERT INTO @tblPhongBanTemp
							SELECT ID FROM NS_PhongBan pb WHERE ID_PhongBanCha IN (SELECT ID FROM @tblPhongBanTemp) 
							DELETE FROM @tblPhongBanTemp WHERE ID IN (SELECT ID FROM @tblPhongBan);
							INSERT INTO @tblPhongBan
							SELECT ID FROM @tblPhongBanTemp
						END
					END

					INSERT INTO @tblNhanVien
					select distinct ct.ID_NhanVien 
					from NS_QuaTrinhCongTac ct					
					where exists (select ID from @tblPhongBan pb where pb.ID= ct.ID_PhongBan)
				end
			else
				INSERT INTO @tblNhanVien values (@ID_NhanVien)
		end		
	RETURN
END");

			Sql(@"CREATE PROCEDURE [dbo].[HD_GetBuTruTraHang]
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
				 select 
						hdt.ID_HoaDon, --- idhdgoc 
						hdt.ID,				
						hdDoi.ID as ID_HDDoi,
						hdt.ID_DoiTuong,
						hdDoi.PhaiThanhToan as HDDoi_PhaiThanhToan,
						hdDoi.NgayLapHoaDon as HDDoi_NgayLapHoaDon
			   from dbo.BH_HoaDon hdt  
			   left join BH_HoaDon hdDoi on hdt.ID = hdDoi.ID_HoaDon  and hdDoi.LoaiHoaDon = 1 and hdDoi.ChoThanhToan ='0'
			   where exists (select ID from @tblHD tblHD where hdt.ID_HoaDon = tblHD.ID_HoaDonGocBanDau and tblHD.ID_HoaDonGocBanDau is not null) ----- get all trahang of hdGoc
			   and hdt.LoaiHoaDon = @LoaiHoaDonTra and hdt.ChoThanhToan ='0'	  
			
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
	from dbo.BH_HoaDon hdt  
	left join BH_HoaDon hdDoi on hdt.ID = hdDoi.ID_HoaDon  and hdDoi.LoaiHoaDon = 1 and hdDoi.ChoThanhToan ='0'
	where exists (select ID from @tblID tbl where hdt.ID_HoaDon = tbl.ID)
	and hdt.LoaiHoaDon = @LoaiHoaDonTra and hdt.ChoThanhToan ='0'	
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
		


			select 
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
						when tblLast.CongNoHDGocCu = 0 then --tblLast.CongNoLuyKeTra - tblLast.CongNoLuyKeDoi
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
			hd.MaHoaDon,
			hd.NgayLapHoaDon,
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
		join BH_HoaDon hd on tblHD.ID= hd.ID
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
	) tblLast order by tblLast.NgayLapHoaDon desc


	  drop table #thuDH
	  drop table #tblNoTraHang
	  drop table #allHDDoi
	  drop table #tblLuyKeTra
	  drop table #tblThuChinhNo

 
END");

			Sql(@"CREATE PROCEDURE [dbo].[TinhCongNo_HDTra]
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
	left join BH_HoaDon hdd on hd.ID = hdd.ID_HoaDon
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
				iif(HDTra_PhaiThanhToan > HDDoi_PhaiThanhToan, HDTra_PhaiThanhToan - HDDoi_PhaiThanhToan,HDTra_PhaiThanhToan)
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
			tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHangMua - tblQD.GiamGiaHangTra) as DoanhThuThuan,
			tblQD.GiaVonHangMua - tblQD.GiaVonHangTra as  TienVon,
			tblQD.TongThueHangMua - tblQD.TongThueHangTra as  TienThue,
			tblQD.GiaTriMua - tblQD.GiaTriTra - (tblQD.GiamGiaHangMua - tblQD.GiamGiaHangTra) - (tblQD.GiaVonHangMua - tblQD.GiaVonHangTra) - tblQD.ChiPhi  as  LaiLo,
			tblQD.GiamGiaHangMua,
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
			cast(c.TongTienThue as float) as TongTienThue,
			iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
			cast(c.ThanhTien - c.GiamGiaHD as float) as DoanhThuThuan,
			iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.TienVon - c.ChiPhi as float),0) as LaiLo,
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
			sum(b.TongTienThue) as TongTienThue	,
			sum(ChiPhi) as ChiPhi
		from (
		select 		
			a.ID_LoHang, a.ID_DonViQuiDoi,			
			sum(isnull(a.TienThue,0)) as TongTienThue,
			sum(isnull(a.GiamGiaHD,0)) as GiamGiaHD,
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
			@TongTienVon FLOAT, @TongLaiLo FLOAT, @SumTienThue FLOAT,@TongDoanhThuThuan FLOAT,@TongChiPhi float		

			SELECT @Rows = Count(*), @TongSoLuong = SUM(SoLuong),
			@TongChietKhau = SUM(TienChietKhau),
			@TongTienTruocCK = SUM(ThanhTienTruocCK),
			@TongThanhTien = SUM(ThanhTien), @TongGiamGiaHD = SUM(GiamGiaHD),
			@TongTienVon = SUM(TienVon), @TongLaiLo = SUM(LaiLo), @SumTienThue = SUM(TongTienThue),
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
END

--BaoCaoBanHang_TongHop_Page2 1,10,'','2022-01-01','2022-10-01','d93b17ea-89b9-4ecf-b242-d03b8cde71de','1,2,3','%1%', '%0%',null,'1,25,19','28fef5a1-f0f2-4b94-a4ad-081b227f3b77','SoLuong','DESC'");

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoDichVu_NhapXuatTon]
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
    @ID_NhomHang_SP [nvarchar](max)
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

    SELECT
    	a.MaHangHoa as MaHangHoa,
    	MAX(a.TenHangHoaFull) as TenHangHoaFull,
    	MAX(a.TenHangHoa) as TenHangHoa,
    	MAX(a.ThuocTinh_GiaTri) as ThuocTinh_GiaTri,
    	MAX(a.TenDonViTinh) as TenDonViTinh,
    	a.TenLoHang as TenLoHang,
    	CAST(ROUND((SUM(a.SoLuongBanDK - a.SoLuongSuDungDK - a.SoLuongTraDK)), 2) as float) as SoLuongConLaiDK,
    	CAST(ROUND((SUM(a.GiaTriBanDK - a.GiaTriSuDungDK - a.GiaTriTraDK)), 0) as float) as GiaTriConLaiDK,
    	CAST(ROUND((SUM(a.SoLuongBanGK)), 2) as float) as SoLuongBanGK, 
    	CAST(ROUND((SUM(a.GiaTriBanGK)), 0) as float) as GiaTriBanGK, 
		CAST(ROUND((SUM(a.SoLuongTraGK)), 2) as float) as SoLuongTraGK, 
    	CAST(ROUND((SUM(a.GiaTriTraGK)), 0) as float) as GiaTriTraGK, 
    	CAST(ROUND((SUM(a.SoLuongSuDungGK)), 2) as float) as SoLuongSuDungGK, 
    	CAST(ROUND((SUM(a.GiaTriSuDungGK)), 0) as float) as GiaTriSuDungGK,
    	CAST(ROUND((SUM(a.SoLuongBanDK - a.SoLuongSuDungDK - a.SoLuongTraDK + a.SoLuongBanGK - a.SoLuongSuDungGK - a.SoLuongTraGK)), 2) as float) as SoLuongConLaiCK,
    	CAST(ROUND((SUM(a.GiaTriBanDK - a.GiaTriSuDungDK - a.GiaTriTraDK + a.GiaTriBanGK - a.GiaTriSuDungGK - a.GiaTriTraGK)), 0) as float) as GiaTriConLaiCK
    	FROM
    	(
		Select
    	dvqd.MaHangHoa,
    	concat(hh.TenHangHoa , dvqd.ThuocTinhGiaTri) as TenHangHoaFull,
    	hh.TenHangHoa,
    	dvqd.TenDonViTinh as TenDonViTinh,
    	dvqd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
    	Case when lh.MaLoHang is null then '' else lh.MaLoHang end as TenLoHang,
    	Max(dt.MaDoiTuong) as MaKhachHang,
    	Max(dt.TenDoiTuong) as TenKhachHang,
    	Max(dt.DienThoai) as DienThoai,
    	Sum(td.SoLuongBanDK) as SoLuongBanDK,
		Sum(td.GiaTriBanDK) as GiaTriBanDK,
		Sum(td.SoLuongTraDK) as SoLuongTraDK,
		Sum(td.GiaTriTraDK) as GiaTriTraDK,
    	Sum(td.SoLuongSuDungDK) as SoLuongSuDungDK,
		sum(td.GiaTriSuDungDK) as GiaTriSuDungDK,
    	
		Sum(td.SoLuongBanGK) as SoLuongBanGK,
		Sum(td.GiaTriBanGK) as GiaTriBanGK,
		Sum(td.SoLuongTraGK) as SoLuongTraGK,
		Sum(td.GiaTriTraGK) as GiaTriTraGK,
    	Sum(td.SoLuongSuDungGK) as SoLuongSuDungGK,
		sum(td.GiaTriSuDungGK) as GiaTriSuDungGK,
	
    	Case When Max(hh.ID_NhomHang) is null then '00000000-0000-0000-0000-000000000000' else Max(hh.ID_NhomHang) end as ID_NhomHang,
		Case when GETDATE() <= Max(td.HanSuDungGoiDV) then 1 else 0 end as ThoiHan
    	FROM
    	(
    	 ---- Đầu kỳ
			Select 
			ctm.MaHoaDon,
			ctm.HanSuDungGoiDV,
			ctm.ID_DoiTuong,
			ctm.ID_DonVi,
			ctm.ID_DonViQuiDoi,
			ctm.ID_LoHang,
			ctm.SoLuong as SoLuongBanDK,
			ctm.ThanhTien as GiaTriBanDK,
			isnull(tbl.SoLuongTra,0) as SoLuongTraDK,
			isnull(tbl.GiaTriTra,0) as GiaTriTraDK,
			isnull(tbl.SoLuongSuDung,0) as SoLuongSuDungDK,
			isnull(tbl.GiaTriSuDung,0) as GiaTriSuDungDK,			
			0 as SoLuongBanGK,
			0 as GiaTriBanGK,
			0 as SoLuongTraGK,
			0 as GiaTriTraGK,
			0 as SoLuongSuDungGK,
			0 as GiaTriSuDungGK
			FROM @tblCTMua_DauKy ctm
			left join (
						select 
							tblSD.ID_ChiTietGoiDV,
							sum(tblSD.SoLuongTra) as SoLuongTra,
							sum(tblSD.GiaTriTra) as GiaTriTra,
							sum(tblSD.SoLuongSuDung) as SoLuongSuDung,
							sum(tblSD.GiaTriSuDung) as GiaTriSuDung
						from 
						(
							---- hdsudung
							Select 								
								ct.ID_ChiTietGoiDV,														
								0 as SoLuongTra,
								0 as GiaTriTra,
								ct.SoLuong as SoLuongSuDung,
								ct.SoLuong * ct.DonGia as GiaTriSuDung
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							join @tblCTMua_DauKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
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
							join @tblCTMua_DauKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)							
							)tblSD group by tblSD.ID_ChiTietGoiDV
			)tbl
			on ctm.ID= tbl.ID_ChiTietGoiDV

			union all
			---- giua ky
			Select 
			ctm.MaHoaDon,
			ctm.HanSuDungGoiDV,
			ctm.ID_DoiTuong,
			ctm.ID_DonVi,
			ctm.ID_DonViQuiDoi,
			ctm.ID_LoHang,								
			0 as SoLuongBanDK,
			0 as GiaTriBanDK,
			0 as SoLuongTraDK,
			0 as GiaTriTraDK,
			0 as SoLuongSuDungDK,
			0 as GiaTriSuDungDK,
			ctm.SoLuong as SoLuongBanDK,
			ctm.ThanhTien as GiaTriBanDK,	
			isnull(tbl.SoLuongTra,0) as SoLuongTraGK,
			isnull(tbl.GiaTriTra,0) as GiaTriTraGK,
			isnull(tbl.SoLuongSuDung,0) as SoLuongSuDungGK,
			isnull(tbl.GiaTriSuDung,0) as GiaTriSuDungGK
			FROM @tblCTMua_GiuaKy ctm
			left join (
						select 
							tblSD.ID_ChiTietGoiDV,
							sum(tblSD.SoLuongTra) as SoLuongTra,
							sum(tblSD.GiaTriTra) as GiaTriTra,
							sum(tblSD.SoLuongSuDung) as SoLuongSuDung,
							sum(tblSD.GiaTriSuDung) as GiaTriSuDung
						from 
						(
							---- hdsudung
							Select 								
								ct.ID_ChiTietGoiDV,														
								0 as SoLuongTra,
								0 as GiaTriTra,
								ct.SoLuong as SoLuongSuDung,
								ct.SoLuong * ct.DonGia as GiaTriSuDung
							FROM BH_HoaDon hd
							join BH_HoaDon_ChiTiet ct on hd.ID = ct.ID_HoaDon
							join @tblCTMua_GiuaKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
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
							join @tblCTMua_GiuaKy ctm on ctm.ID= ct.ID_ChiTietGoiDV
							where hd.ChoThanhToan= 0
							and hd.LoaiHoaDon = 6
							and (ct.ID_ChiTietDinhLuong = ct.ID or ct.ID_ChiTietDinhLuong is null)							
							)tblSD group by tblSD.ID_ChiTietGoiDV
			)tbl
			on ctm.ID= tbl.ID_ChiTietGoiDV					
    	)as td
    	inner join DonViQuiDoi dvqd on td.ID_DonViQuiDoi = dvqd.ID
    		inner join DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID
    		left join DM_DoiTuong dt on td.ID_DoiTuong = dt.ID
    		left join DM_LoHang lh on td.ID_LoHang = lh.ID
    		where td.ID_DonVi in (select * from splitstring(@ID_ChiNhanh))
    		and hh.LaHangHoa like @LaHangHoa
    		and hh.TheoDoi like @TheoDoi
    		and (dvqd.MaHangHoa like @Text_Search or dvqd.MaHangHoa like @MaHH or hh.TenHangHoa_KhongDau like @MaHH or hh.TenHangHoa_KyTuDau like @MaHH)
			and (dt.TenDoiTuong_KhongDau like @MaKH or dt.TenDoiTuong_ChuCaiDau like @MaKH or dt.DienThoai like @MaKH or dt.MaDoiTuong like @MaKH or dt.MaDoiTuong like @MaKH_TV)
			and dvqd.Xoa like @TrangThai
			Group by td.MaHoaDon, dvqd.MaHangHoa, hh.TenHangHoa, dvqd.TenDonViTinh, dvqd.ThuocTinhGiaTri, lh.MaLoHang, dvqd.Xoa
    		) as a
    		where (a.ID_NhomHang like @ID_NhomHang or a.ID_NhomHang in (select * from splitstring(@ID_NhomHang_SP)))
			and a.ThoiHan like @ThoiHan
    		Group by a.MaHangHoa, TenLoHang
    		ORDER BY SUM(a.GiaTriBanDK - a.GiaTriSuDungDK + a.GiaTriBanGK - a.GiaTriSuDungGK) DESC
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
		GiamGiaHD float)
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
			iif(@XemGiaVon='0',cast( 0 as float),round( sum(a.GiaVon),2)) as GiaVon,
			round(sum(a.SoLuong) -  sum(a.SoLuongTra) - sum(a.SoLuongSuDung),2) as SoLuongConLai,
			CAST(ROUND(Case when DATEADD(day,-1,GETDATE()) <= MAX(a.HanSuDungGoiDV)
				then DATEDIFF(day,DATEADD(day,-1,GETDATE()),MAX(a.HanSuDungGoiDV)) else 0 end, 0) as float) as SoNgayConHan, 
			CAST(ROUND(Case when DATEADD(day,-1,GETDATE()) > MAX(a.HanSuDungGoiDV) 
			then DATEDIFF(day,DATEADD(day,-1,GETDATE()) ,MAX(a.HanSuDungGoiDV)) * (-1) else 0 end, 0) as float) as SoNgayHetHan			
		From
		(
				---- get by idnhom, thoihan --> check where
				select *
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
						isnull(tbl.SoLuongTra,0) as SoLuongTra,
						isnull(tbl.GiaTriTra,0) as GiaTriTra,
						isnull(tbl.SoLuongSuDung,0) as SoLuongSuDung,
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
								ct.ThanhTien as GiaTriTra,
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_ChiTietHangXuat]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
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
	
			select 
				dv.MaDonVi, dv.TenDonVi, nv.TenNhanVien,
				tblQD.NgayLapHoaDon, tblQD.MaHoaDon,
				tblQD.BienSo,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				tblQD.GhiChu,
				tblQD.DienGiai,
				round(SoLuong,3) as SoLuong,
				iif(@XemGiaVon='1', round(GiaTriXuat,3),0) as ThanhTien,
				case tblQD.LoaiXuatKho
					when 1 then N'Hóa đơn bán lẻ'
					when 2 then N'Xuất sử dụng gói dịch vụ'
					when 3 then N'Xuất bán dịch vụ định lượng'
					when 11 then N'Xuất sửa chữa'
					when 7 then N'Trả hàng nhà cung cấp'
					when 8 then N'Xuất kho'
					when 9 then N'Phiếu kiểm kê'
					when 10 then N'Chuyển hàng'				
				end as LoaiHoaDon
			from
			(
					select 
						qd.ID_HangHoa,
						tblHD.ID_LoHang, 
						tblHD.ID_DonVi,
						tblHD.ID_CheckIn,
						tblHD.NgayLapHoaDon,
						tblHD.MaHoaDon, 
						tblHD.ID_NhanVien,
						tblHD.LoaiHoaDon,
						tblHD.LoaiXuatKho,
						tblHD.BienSo,
						tblHD.DienGiai,
						iif(@SearchString='',tblHD.DienGiai,dbo.FUNC_ConvertStringToUnsign(tblHD.DienGiai)) as DienGiaiUnsign,
						max(tblHD.GhiChu) as GhiChu,
						iif(@SearchString='',max(tblHD.GhiChu),max(dbo.FUNC_ConvertStringToUnsign(tblHD.GhiChu))) as GhiChuUnsign,
						sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
						sum(tblHD.GiaTriXuat) as GiaTriXuat
					from
					(
						select 
							hd.NgayLapHoaDon, 
							hd.MaHoaDon,
							hd.LoaiHoaDon,
							hd.ID_HoaDon,
							hd.ID_CheckIn,
							hd.ID_NhanVien, 
							hd.DienGiai, 
							xe.BienSo,
							case hd.LoaiHoaDon
								when 8 then case when hd.ID_PhieuTiepNhan is not null then case when ct.ChatLieu= 4 then 2 else  11 end -- xuat suachua
									else 8 end
								when 1 then case when hd.ID_HoaDon is null and ct.ID_ChiTietGoiDV is not null then 2 --- xuat sudung gdv
									else case when ct.ID_ChiTietDinhLuong is not null then 3 --- xuat ban dinhluong
									else 1 end end -- xuat banle
								else hd.LoaiHoaDon end as LoaiXuatKho, -- xuat khac: traNCC, chuyenang,..
							ct.ID_ChiTietGoiDV,
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							case hd.LoaiHoaDon
							when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0)
							when 10 then ct.TienChietKhau else ct.SoLuong end as SoLuong,
							ct.TienChietKhau,
							ct.GiaVon,
							ct.GhiChu,
							case hd.LoaiHoaDon
								when 7 then ct.SoLuong * ct.DonGia
								when 10 then ct.TienChietKhau * ct.DonGia --- chuyenhang: always get DonGia
								when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0) * ct.GiaVon 
								else ct.SoLuong* ct.GiaVon end as GiaTriXuat
						from BH_HoaDon_ChiTiet ct
						join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
						left join Gara_PhieuTiepNhan tn on hd.ID_PhieuTiepNhan = tn.ID
						left join Gara_DanhMucXe xe on tn.ID_Xe= xe.ID
						WHERE hd.ChoThanhToan = 0 
						AND hd.NgayLapHoaDon between @timeStart AND @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						and hd.LoaiHoaDon not in (3,4,6,19,25)
						and (ct.ChatLieu is null or ct.ChatLieu !='5')
						and iif(hd.LoaiHoaDon=9,ct.SoLuong, -1) < 0 -- phieukiemke: chi lay neu soluong < 0 (~xuatkho)
					) tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID			
				where exists (select Name from dbo.splitstring(@LoaiChungTu) loaict where tblHD.LoaiXuatKho = loaict.Name)				
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi,
				 tblHD.ID_CheckIn,tblHD.NgayLapHoaDon, tblHD.MaHoaDon, tblHD.ID_NhanVien, 
				 tblHD.LoaiHoaDon, tblHD.LoaiXuatKho, tblHD.DienGiai, tblHD.BienSo			
			)tblQD
			join DM_DonVi dv on tblQD.ID_DonVi = dv.ID
			join DM_LoaiChungTu ct on tblQD.LoaiHoaDon = ct.ID
			left join NS_NhanVien nv on tblQD.ID_NhanVien= nv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or (tblQD.ID_LoHang is null and lo.ID is null))
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
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%'
			or nv.TenNhanVien like '%'+b.Name+'%'
			or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
			or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
			or nv.MaNhanVien like '%'+b.Name+'%'
			or tblQD.GhiChu like N'%'+b.Name+'%'
			or tblQD.MaHoaDon like N'%'+b.Name+'%'
			or tblQD.BienSo like N'%'+b.Name+'%'
			or tblQD.GhiChuUnsign like '%'+b.Name+'%'
			or tblQD.DienGiai like N'%'+b.Name+'%'
			or tblQD.DienGiaiUnsign like N'%'+b.Name+'%'
			)=@count or @count=0)
			order by tblQD.NgayLapHoaDon desc


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
						when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang))
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
							when 4 then iif( hd.TongTienHang = 0,0, ct.SoLuong* (ct.DonGia - ct.TienChietKhau) * (1- hd.TongGiamGia/hd.TongTienHang)) -- sum(ThanhTien) cthd -  TongGiamGia hd
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

			Sql(@"ALTER PROCEDURE [dbo].[BaoCaoKho_TongHopHangXuat]
    @ID_DonVi NVARCHAR(MAX),
    @timeStart [datetime],
	@timeEnd [datetime],
    @SearchString [nvarchar](max),
    @ID_NhomHang [uniqueidentifier],
    @TheoDoi [nvarchar](max),
    @TrangThai [nvarchar](max),
    @ID_NguoiDung [uniqueidentifier],
	@LoaiChungTu [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;
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

			select 
				dv.MaDonVi, dv.TenDonVi,
				isnull(nhom.TenNhomHangHoa, N'Nhóm Hàng Hóa Mặc Định') as TenNhomHang,
				isnull(lo.MaLoHang,'') as TenLoHang,
				qd.MaHangHoa, qd.TenDonViTinh, 
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				hh.TenHangHoa,
				CONCAT(hh.TenHangHoa,qd.ThuocTinhGiaTri) as TenHangHoaFull,
				round(SoLuong,3) as SoLuong,
				iif(@XemGiaVon='1', round(ThanhTien,3),0) as ThanhTien
			from
			(				
				select 
					qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi,
					sum(tblHD.SoLuong * iif(qd.TyLeChuyenDoi=0,1, qd.TyLeChuyenDoi)) as SoLuong,
					sum(GiaTriXuat) as ThanhTien
				from
				(
						select 
							hd.NgayLapHoaDon, 
							hd.MaHoaDon,
							hd.LoaiHoaDon,
							hd.ID_HoaDon,
							case hd.LoaiHoaDon
								when 8 then case when hd.ID_PhieuTiepNhan is not null then case when ct.ChatLieu= 4 then 2 else 11 end -- xuat suachua
									else 8 end
								when 1 then case when hd.ID_HoaDon is null and ct.ID_ChiTietGoiDV is not null then 2 --- xuat sudung gdv
									else case when ct.ID_ChiTietDinhLuong is not null then 3 --- xuat ban dinhluong
									else 1 end end -- xuat banle
								else hd.LoaiHoaDon end as LoaiXuatKho, -- xuat khac: traNCC, chuyenang,..
							ct.ID_ChiTietGoiDV,
							ct.ID_DonViQuiDoi,
							ct.ID_LoHang,
							hd.ID_DonVi,
							case hd.LoaiHoaDon
							when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0)
							when 10 then ct.TienChietKhau else ct.SoLuong end as SoLuong,
							ct.TienChietKhau,
							ct.GiaVon,
							case hd.LoaiHoaDon
								when 7 then ct.SoLuong * ct.DonGia
								when 10 then ct.TienChietKhau * ct.DonGia
								when 9 then iif(ct.SoLuong < 0, -ct.SoLuong, 0) * ct.GiaVon
								else ct.SoLuong* ct.GiaVon end as GiaTriXuat
						from BH_HoaDon_ChiTiet ct
						join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
						WHERE hd.ChoThanhToan = 0 
						AND hd.NgayLapHoaDon between @timeStart AND  @timeEnd
						and exists (select ID from @tblChiNhanh dv where hd.ID_DonVi = dv.ID)
						and hd.LoaiHoaDon not in (3,4,6,19,25)
						and (ct.ChatLieu is null or ct.ChatLieu !='5')
						and iif(hd.LoaiHoaDon=9,ct.SoLuong, -1) < 0 -- phieukiemke: chi lay neu soluong < 0 (~xuatkho)
				) tblHD
				join DonViQuiDoi qd on tblHD.ID_DonViQuiDoi= qd.ID			
				where exists (select Name from dbo.splitstring(@LoaiChungTu) loaict where tblHD.LoaiXuatKho = loaict.Name)				
				group by qd.ID_HangHoa,tblHD.ID_LoHang, tblHD.ID_DonVi		
			)tblQD
			join DM_DonVi dv on tblQD.ID_DonVi = dv.ID
			join DM_HangHoa hh on tblQD.ID_HangHoa= hh.ID
			join DonViQuiDoi qd on hh.ID= qd.ID_HangHoa and qd.LaDonViChuan=1
			left join DM_NhomHangHoa nhom on hh.ID_NhomHang= nhom.ID
			left join DM_LoHang lo on tblQD.ID_LoHang= lo.ID and (lo.ID= tblQD.ID_LoHang or (tblQD.ID_LoHang is null and lo.ID is null))
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
			or dv.MaDonVi like '%'+b.Name+'%'
			or dv.TenDonVi like '%'+b.Name+'%')=@count or @count=0)
		order by dv.TenDonVi, hh.TenHangHoa, lo.MaLoHang	


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
    		Case when hd.TongTienHang = 0 then 0 else ct.ThanhTien * ((hd.TongGiamGia + isnull(hd.KhuyeMai_GiamGia,0)) / hd.TongTienHang) end as GiamGiaHD
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

			Sql(@"ALTER PROCEDURE [dbo].[GetAll_TonKhoDauKy]
    @IDDonVis [nvarchar](max),
    @ToDate [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	DECLARE @tblChiNhanh TABLE(ID UNIQUEIDENTIFIER);
    	INSERT INTO @tblChiNhanh SELECT Name FROM splitstring(@IDDonVis)

		select 
				ID_DonViInput,			
				ID_HangHoa,		
    			ID_LoHang,
				TonLuyKe,
				GiaVon
		from
		(
		select 
				tblUnion.ID_DonViInput, 
				tblUnion.LoaiHoaDon,
				tblUnion.NgayLapHoaDon,
				dvqd.ID_HangHoa,
				tblUnion.ID_LoHang,
				tblUnion.TonLuyKe,
				tblUnion.GiaVon / ISNULL(dvqd.TyLeChuyenDoi,1) as GiaVon,
				ROW_NUMBER() OVER (PARTITION BY dvqd.ID_HangHoa, tblUnion.ID_LoHang, tblUnion.ID_DonViInput ORDER BY tblUnion.NgayLapHoaDon DESC) AS RN
		from
		(
			select *
			from
			(
				SELECT hd.ID_DonVi as ID_DonViInput, 					
						hd.LoaiHoaDon,
						hd.NgayLapHoaDon,					
						hdct.ID_DonViQuiDoi,
						hdct.ID_LoHang,
						hdct.TonLuyKe,
						hdct.GiaVon
    			FROM BH_HoaDon_ChiTiet hdct
    			JOIN BH_HoaDon hd ON hd.ID = hdct.ID_HoaDon	
    			where hd.ChoThanhToan = 0  
				AND hd.LoaiHoaDon IN (1, 5, 7, 8, 4, 6, 9,18, 13,14)
    			and hd.ID_DonVi in (select ID from @tblChiNhanh dv)
				and hd.NgayLapHoaDon < @ToDate

				union all

				SELECT
						cn.ID as ID_DonViInput,
						hd.LoaiHoaDon,
						IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hd.NgaySua, hd.NgayLapHoaDon) as NgayLapHoaDon,
						hdct.ID_DonViQuiDoi,
						hdct.ID_LoHang,
						IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hdct.TonLuyKe_NhanChuyenHang, hdct.TonLuyKe) AS TonKho, 
    					IIF(ID_CheckIn = cn.ID and hd.YeuCau='4', hdct.GiaVon_NhanChuyenHang, hdct.GiaVon) AS GiaVon
    			FROM BH_HoaDon_ChiTiet hdct
    			JOIN BH_HoaDon hd ON hd.ID = hdct.ID_HoaDon
				JOIN @tblChiNhanh cn ON cn.ID = hd.ID_DonVi OR (hd.ID_CheckIn = cn.ID and hd.YeuCau = '4')		
    			where hd.ChoThanhToan = 0 
				AND hd.LoaiHoaDon = 10
    			and (hd.ID_CheckIn in (select ID from @tblChiNhanh)
				or (hd.ID_DonVi in (select ID from @tblChiNhanh )))
				and (hd.NgayLapHoaDon < @ToDate or hd.NgaySua < @ToDate)
			) tblCheck where tblCheck.NgayLapHoaDon < @ToDate
		) tblUnion
		JOIN DonViQuiDoi dvqd ON tblUnion.ID_DonViQuiDoi = dvqd.ID	
		) tblRN where tblRN.RN = 1

END");

			Sql(@"ALTER PROCEDURE [dbo].[GetAllBangLuong]
    @IDChiNhanhs [nvarchar](max),
    @TxtSearch [nvarchar](max),
    @FromDate [datetime],
    @ToDate [datetime],
    @TrangThais [nvarchar](10),
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    
   DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TxtSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	DECLARE @tblChiNhanh TABLE (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select Name from dbo.splitstring(@IDChiNhanhs);
    
    	with data_cte
    	as (
    	select bl.*, 		
			soquy.DaTra,
			soquy.LuongThucNhan,
			soquy.TruTamUngLuong,
			soquy.ThanhToan,
			soquy.LuongThucNhan - soquy.DaTra as ConLai
    	from NS_BangLuong bl
		
    	join (select ct.ID_BangLuong,
    				sum(ct.LuongThucNhan) as LuongThucNhan,
    				sum(isnull(soquy.TienThu,0)) as ThanhToan,
					sum(isnull(soquy.TruTamUngLuong,0)) as TruTamUngLuong,
					sum(isnull(soquy.TienThu,0)) +	sum(isnull(soquy.TruTamUngLuong,0)) as DaTra
    			from NS_BangLuong_ChiTiet ct
    			left join( select qct.ID_BangLuongChiTiet , 
						sum(qct.TienThu) as TienThu,
						sum(ISNULL(qct.TruTamUngLuong,0)) as TruTamUngLuong
    						from Quy_HoaDon_ChiTiet qct  
    						join Quy_HoaDon qhd on qct.ID_HoaDon = qhd.ID where qhd.TrangThai= 1 
    						group by  qct.ID_BangLuongChiTiet) soquy on ct.ID= soquy.ID_BangLuongChiTiet					
    			group by ct.ID_BangLuong
    			) soquy on bl.ID = soquy.ID_BangLuong
    	where exists (select Name from dbo.splitstring(@TrangThais) tt where bl.TrangThai = tt.Name)
    	and exists (select ID from @tblChiNhanh dv where bl.ID_DonVi= dv.ID)
    	and ((bl.TuNgay >= @FromDate and (bl.TuNgay <= @ToDate or bl.DenNgay <= @ToDate))
    		or ( bl.DenNgay <= @ToDate and ( bl.DenNgay >= @FromDate or bl.TuNgay >= @FromDate))
    			)
    	AND ((select count(Name) from @tblSearchString b where 
    					bl.MaBangLuong like '%'+b.Name+'%' 
    					or bl.TenBangLuong like '%'+b.Name+'%' 
    						or bl.GhiChu like '%'+b.Name+'%' 
    				
    						)=@count or @count=0)	
    	),
    	count_cte
    	as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(LuongThucNhan) as TongPhaiTra,		
					sum(TruTamUngLuong) as TongTamUng,
					sum(ThanhToan) as TongThanhToan,
    				sum(DaTra) as TongDaTra,
    				sum(ConLai) as TongConLai
    			from data_cte
    		)
    		select dt.*, cte.*, ISNULL(nv.TenNhanVien,'') as NguoiDuyet,
				dv.SoDienThoai AS DienThoaiChiNhanh,
				dv.DiaChi AS DiaChiChiNhanh
    		from data_cte dt
			join DM_DonVi dv on dt.ID_DonVi= dv.ID
    		left join NS_NhanVien nv on dt.ID_NhanVienDuyet = nv.ID
    		cross join count_cte cte
    		order by dt.DenNgay  desc, dt.NgayTao desc
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetDSGoiDichVu_ofKhachHang]
    @IDChiNhanhs [nvarchar](50) = null,
	@IDCustomers [nvarchar](max) = null,
	@IDCars nvarchar(max) = null,
	@TextSearch nvarchar(max) = null,
	@DateFrom datetime = null,
	@DateTo datetime = null    
AS
BEGIN
    SET NOCOUNT ON;
	declare @sql nvarchar(max)='', @where nvarchar(max)='', @whereOut nvarchar(max) ='', @paramDefined nvarchar(max)=''
	declare @tbldefined nvarchar(max) =' declare @tblChiNhanh table(ID uniqueidentifier) 
								declare @tblCus table(ID uniqueidentifier)
								declare @tblCar table(ID uniqueidentifier) 
								declare @countChiNhanh int = (select count(ID) from DM_DonVi)'

	set @whereOut =' where 1 = 1 and ctm.ChatLieu != 5				
				and (ctm.ID_ChiTietDinhLuong is null or ctm.ID= ctm.ID_ChiTietDinhLuong) '

	if isnull(@IDChiNhanhs,'')!=''
		begin
			set @sql = CONCAT(@sql, ' insert into @tblChiNhanh select name from dbo.splitstring(@IDChiNhanh_In) ')
			set @where= CONCAT(@where, ' and exists (select ID from @tblChiNhanh cn where cn.ID = hd.ID_DonVi)')
		end

	if isnull(@IDCustomers,'')!=''
		begin			
			set @where = CONCAT(@where , ' and exists (select ID from @tblCus cus where hd.ID_DoiTuong = cus.ID)')
			set @sql = CONCAT(@sql, ' insert into @tblCus select name from dbo.splitstring(@IDCustomers_In) ;')
		end
	if isnull(@IDCars,'')!=''
		begin
			set @where = CONCAT(@where , ' and exists (select ID from @tblCar car where hd.ID_Xe = car.ID)')
			set @sql = CONCAT(@sql, ' insert into @tblCar select name from dbo.splitstring(@IDCars_In) ;')
		end

	if isnull(@TextSearch,'')!=''
		set @whereOut= CONCAT(@whereOut, ' and (tbl.MaHoaDon like N''%'' + @TextSearch_In + ''%'' 
			or qd.MaHangHoa like N''%'' + @TextSearch_In + ''%''  or hh.TenHangHoa like N''%'' + @TextSearch_In + ''%''
			 or hh.TenHangHoa_KhongDau like N''%'' + @TextSearch_In + ''%'')    ')

	if isnull(@DateFrom,'')!=''
		set @where= CONCAT(@where, ' and (hd.HanSuDungGoiDV is null or hd.HanSuDungGoiDV >= @DateFrom_In)   ')

	if isnull(@DateTo,'')!=''
		set @where= CONCAT(@where, ' and (hd.HanSuDungGoiDV is not null and hd.HanSuDungGoiDV < @DateTo_In)   ')

	set @sql = concat(@tbldefined, @sql, '

    select  
    		tbl.ID as ID_GoiDV,
			tbl.MaHoaDon, 
			tbl.ID_Xe,	
			tbl.ID_DonVi,	
			tbl.TenDonVi,	
			@countChiNhanh as SLChiNhanh,
			convert(varchar,tbl.NgayLapHoaDon, 103) as NgayLapHoaDon,
    		convert(varchar,tbl.NgayApDungGoiDV, 103) as NgayApDungGoiDV,
    		convert(varchar,tbl.HanSuDungGoiDV, 103) as HanSuDungGoiDV, 		
    		ctm.ID as ID_ChiTietGoiDV, ctm.ID_DonViQuiDoi, ctm.ID_LoHang, 
    		ISNULL(ctm.ID_TangKem, ''00000000-0000-0000-0000-000000000000'') as ID_TangKem, ISNULL(ctm.TangKem,0) as TangKem, 
			ctm.TienChietKhau ,
			ctm.PTChietKhau ,
    		ctm.DonGia  as GiaBan,
    		ctm.SoLuong, 
    		ctm.SoLuong - ISNULL(ctt.SoLuongTra,0) as SoLuongMua,
    		ISNULL(ctt.SoLuongDung,0) as SoLuongDung,
    		round(ctm.SoLuong - ISNULL(ctt.SoLuongTra,0) - ISNULL(ctt.SoLuongDung,0),2) as SoLuongConLai,		
    		qd.TenDonViTinh,qd.ID_HangHoa,qd.MaHangHoa,ISNULL(qd.LaDonViChuan,0) as LaDonViChuan, CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
    		hh.LaHangHoa, 
			iif(ctm.TenHangHoaThayThe is null or ctm.TenHangHoaThayThe ='''', hh.TenHangHoa, ctm.TenHangHoaThayThe) as TenHangHoa,
			hh.TonToiThieu, CAST(ISNULL(hh.QuyCach,1) as float) as QuyCach,
    		ISNULL(hh.ID_NhomHang,''00000000-0000-0000-0000-000000000001'') as ID_NhomHangHoa,
    		ISNULL(hh.SoPhutThucHien,0) as SoPhutThucHien,
    		case when hh.LaHangHoa = 1 then ''0'' else CAST(ISNULL(hh.ChiPhiThucHien,0) as float) end PhiDichVu,
    		Case when hh.LaHangHoa=1 then ''0'' else ISNULL(hh.ChiPhiTinhTheoPT,''0'') end as LaPTPhiDichVu,
    		ISNULL(hh.GhiChu,'''') as GhiChuHH,
    		ISNULL(ctm.GhiChu,'''') as GhiChu,
    		isnull(hh.QuanLyTheoLoHang,''0'') as QuanLyTheoLoHang,
    		lo.MaLoHang, lo.NgaySanXuat, lo.NgayHetHan, xe.BienSo, 		
			ISNULL(hh.DichVuTheoGio,0) as DichVuTheoGio,
			ISNULL(hh.DuocTichDiem,0) as DuocTichDiem,
			ISNULL(hh.ThoiGianBaoHanh,0) as ThoiGianBaoHanh,
			ISNULL(hh.LoaiBaoHanh,0) as LoaiBaoHanh,			
			ISNULL(hh.ChietKhauMD_NV,0) as ChietKhauMD_NV,
			ISNULL(hh.ChietKhauMD_NVTheoPT,''1'') as ChietKhauMD_NVTheoPT,
			ISNULL(hh.HoaHongTruocChietKhau,0) as HoaHongTruocChietKhau,
			iif(hh.LoaiHangHoa is null, iif(hh.LaHangHoa=1,1,2), hh.LoaiHangHoa) as LoaiHangHoa,
			hh.LaHangHoa,
			ctm.ID_ParentCombo,			
			iif(ctm.ID_ParentCombo = ctm.ID, 0,1) as SoThuTu,
			isnull(ctm.GiaVon,0) as GiaVon
    	from
			(
			select 
					hd.ID,
					hd.MaHoaDon, 
					hd.ID_Xe,	
					hd.ID_DonVi,
					dv.TenDonVi,
					hd.NgayLapHoaDon,				
					hd.NgayApDungGoiDV,
					hd.HanSuDungGoiDV
				from dbo.BH_HoaDon hd
				join DM_DonVi dv on hd.ID_DonVi = dv.ID
				where hd.LoaiHoaDon = 19 and hd.ChoThanhToan = 0 ', @where , 												
			' ) tbl 
		join dbo.BH_HoaDon_ChiTiet ctm on tbl.ID = ctm.ID_HoaDon 
		left join Gara_DanhMucXe xe on tbl.ID_Xe= xe.ID
    	join DonViQuiDoi qd on ctm.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa = hh.ID
    	left join DM_LoHang lo on ctm.ID_LoHang = lo.ID
    	left join 
    	(
    		select a.ID_ChiTietGoiDV,
    			SUM(a.SoLuongTra) as SoLuongTra,
    			SUM(a.SoLuongDung) as SoLuongDung
    		from
    			(-- sum soluongtra
    			select ct.ID_ChiTietGoiDV,
    				SUM(ct.SoLuong) as SoLuongTra,
    				0 as SoLuongDung
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    			where hd.ChoThanhToan= 0 and hd.LoaiHoaDon = 6
    			and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    			group by ct.ID_ChiTietGoiDV
    
    			union all
    			-- sum soluong sudung
    			select ct.ID_ChiTietGoiDV,
    				0 as SoLuongDung,
    				SUM(ct.SoLuong) as SoLuongDung
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd on ct.ID_HoaDon = hd.ID
    			where hd.ChoThanhToan=0 and hd.LoaiHoaDon in (1,25)
    			and (ct.ID_ChiTietDinhLuong is null or ct.ID_ChiTietDinhLuong = ct.ID)
    			group by ct.ID_ChiTietGoiDV
    			) a group by a.ID_ChiTietGoiDV
    	) ctt on ctm.ID = ctt.ID_ChiTietGoiDV ', @whereOut , N' order by tbl.NgayLapHoaDon desc')
	    
		----print @sql
		set @paramDefined =' @IDChiNhanh_In nvarchar(max),
							@IDCustomers_In nvarchar(max),
							@IDCars_In nvarchar(max),
							@TextSearch_in nvarchar(max),
							@DateFrom_In nvarchar(max),
							@DateTo_in nvarchar(max)'
		
			exec sp_executesql @sql, @paramDefined,
							@IDChiNhanh_In = @IDChiNhanhs,
							@IDCustomers_In = @IDCustomers,
							@IDCars_in = @IDCars,
							@TextSearch_In = @TextSearch,
							@DateFrom_In = @DateFrom,
							@DateTo_in = @DateTo

END


---GetDSGoiDichVu_ofKhachHang 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE','ce30b274-bfd9-448d-8142-02a9391f4b0a','668BF4EE-D5EB-4C95-81BF-25CDB4DEE179',N'Xông Hơi Nước'");

			Sql(@"ALTER PROCEDURE [dbo].[getList_HoaDonbyNhanVien]
    @ID_NhanVien [uniqueidentifier]
AS
BEGIN
    select hd.ID, nv.ID as ID_NhanVien, hd.MaHoaDon, dt.TenDoiTuong as TenKhachHang, hd.NgayLapHoaDon, hd.TongTienHang ,
		case hd.LoaiHoaDon
			when 1 then N'Hóa đơn bán'
			when 25 then N'Hóa đơn sửa chữa'
			when 3 then N'Báo giá sửa chữa'
			when 4 then N'Phiếu nhập hàng'
			when 9 then N'Phiếu kiểm kê'
			when 8 then N'Phiếu xuất kho'
			when 18 then N'Điều chỉnh giá vốn'
			
		end as TenLoaiHoaDon
    	from BH_HoaDon hd
    	left join NS_NhanVien nv on hd.ID_NhanVien= nv.ID
    	left join DM_DoiTuong dt on hd.ID_DoiTuong = dt.ID
    	where hd.ID_NhanVien = @ID_NhanVien
		and hd.ChoThanhToan='0'
END");

			Sql(@"ALTER PROCEDURE [dbo].[getlist_HoaDonTraHang]
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
	@PageSize int
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
			

    		ISNULL(bhhd.DiemGiaoDich,0) as DiemGiaoDich,
    		bhhd.ChoThanhToan,
    		ISNULL(vt.TenViTri,'') as TenPhongBan,

    		bhhd.NgayLapHoaDon,
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

			Sql(@"ALTER PROCEDURE [dbo].[GetListCashFlow_Before]
    @IDDonVis [nvarchar](max),
    @ID_NhanVien [nvarchar](40),
    @ID_TaiKhoanNganHang [nvarchar](40),
    @ID_KhoanThuChi [nvarchar](40),
    @DateFrom [datetime],
    @DateTo [datetime],
    @LoaiSoQuy varchar(15),
    @LoaiChungTu [nvarchar](2),
    @TrangThaiSoQuy [nvarchar](2),
    @TrangThaiHachToan [nvarchar](2),
    @TxtSearch [nvarchar](max)
AS
BEGIN
    SET NOCOUNT ON;

	declare @tblDonVi table (ID_DonVi uniqueidentifier)
	insert into @tblDonVi
	select name from dbo.splitstring(@IDDonVis)

	declare @tblLoai table (Loai int)
	insert into @tblLoai
	select name from dbo.splitstring(@LoaiSoQuy)

	--declare @nguoitao nvarchar(100) = (select taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
	--declare @tblNhanVien table (ID uniqueidentifier)
	--insert into @tblNhanVien
	--select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @IDDonVis,'SoQuy_XemDS_PhongBan','SoQuy_XemDS_HeThong');
 
 select 
	sum(ThuMat)- sum(ChiMat) as TongThuMat,  
	sum(ThuGui) - sum(ChiGui) as TongThuCK
 from
 (
 select
	iif(a1.LoaiHoaDon=11, TienMat,0) as ThuMat,
	iif(a1.LoaiHoaDon=12, TienMat,0) as ChiMat,
	iif(a1.LoaiHoaDon=11, TienGui,0) as ThuGui,
	iif(a1.LoaiHoaDon=12, TienGui,0) as ChiGui
 from
 (
    	 select
			tblView.LoaiHoaDon,
			sum(TienMat) as TienMat,
			sum(TienGui) as TienGui
	from
		(select 
			tblQuy.MaHoaDon,		
			tblQuy.LoaiHoaDon,
			ISNUll(tblQuy.TrangThai,'1') as TrangThai,
			tblQuy.NoiDungThu,
			tblQuy.ID_NhanVienPT as ID_NhanVien,			
			TienMat, TienGui, TienMat + TienGui as TienThu,
			TienMat + TienGui as TongTienThu,
			cast(ID_TaiKhoanNganHang as varchar(max)) as ID_TaiKhoanNganHang,
			case when tblQuy.LoaiHoaDon = 11 then '11' else '12' end as LoaiChungTu,
    		case when tblQuy.HachToanKinhDoanh = '1' then '11' else '10' end as TrangThaiHachToan,
    		case when tblQuy.TrangThai = '0' then '10' else '11' end as TrangThaiSoQuy,
			case when tblQuy.TienMat > 0 then case when tblQuy.TienGui > 0 then '2' else '1' end 
			else case when tblQuy.TienGui > 0 then '0'
				else case when ID_TaiKhoanNganHang!='00000000-0000-0000-0000-000000000000' then '0' else '1' end end end as LoaiSoQuy
							
		from
			(select 
				 a.ID_hoaDon, 
				 a.MaHoaDon,			
				 a.LoaiHoaDon,
				 a.HachToanKinhDoanh, 
				 a.NoiDungThu,
				 a.ID_NhanVienPT, a.TrangThai,
				 sum(isnull(a.TienMat, 0)) as TienMat,
				 sum(isnull(a.TienGui, 0)) as TienGui,
				 max(a.ID_DoiTuong) as ID_DoiTuong,
				 max(a.ID_NhanVien) as ID_NhanVien,
				 max(a.ID_TaiKhoanNganHang) as ID_TaiKhoanNganHang			
			from
			(				
					select qhd.MaHoaDon,				
					qhd.LoaiHoaDon,
					qhd.HachToanKinhDoanh, qhd.PhieuDieuChinhCongNo, qhd.NoiDungThu,
					qhd.ID_NhanVien as ID_NhanVienPT, qhd.TrangThai,
					qct.ID_HoaDon, 
					iif(qct.HinhThucThanhToan= 1, qct.TienThu,0) as TienMat,
					iif(qct.HinhThucThanhToan= 2 or qct.HinhThucThanhToan = 3, qct.TienThu,0) as TienGui,		
					qct.ID_DoiTuong, qct.ID_NhanVien, 
					ISNULL(qct.ID_TaiKhoanNganHang,'00000000-0000-0000-0000-000000000000') as ID_TaiKhoanNganHang,
					ISNULL(qct.ID_KhoanThuChi,'00000000-0000-0000-0000-000000000000') as ID_KhoanThuChi
					from Quy_HoaDon_ChiTiet qct
					join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
					join @tblDonVi cn on qhd.ID_DonVi= cn.ID_DonVi
					left join DM_TaiKhoanNganHang tk on qct.ID_TaiKhoanNganHang= tk.ID
					left join Quy_KhoanThuChi ktc on qct.ID_KhoanThuChi= ktc.ID
					where qhd.NgayLapHoaDon < @DateFrom
					and qct.HinhThucThanhToan not in (4,5,6)
					and (qhd.PhieuDieuChinhCongNo !='1' or qhd.PhieuDieuChinhCongNo is null)
					and (@ID_TaiKhoanNganHang  ='%%' Or qct.ID_TaiKhoanNganHang like @ID_TaiKhoanNganHang)
					and (@ID_KhoanThuChi ='%%' or qct.ID_KhoanThuChi like @ID_KhoanThuChi)				
			) a
			 group by a.ID_HoaDon, a.MaHoaDon, 
				a.LoaiHoaDon,
				a.HachToanKinhDoanh, a.PhieuDieuChinhCongNo, a.NoiDungThu,
				a.ID_NhanVienPT , a.TrangThai
		) tblQuy
		--left join DM_DoiTuong dt on tblQuy.ID_DoiTuong = dt.ID
	 ) tblView
	 where tblView.TrangThaiHachToan like '%'+ @TrangThaiHachToan + '%'
	-- and tblView.MaHoaDon not like 'CB%'		
    	and tblView.TrangThaiSoQuy like '%'+ @TrangThaiSoQuy + '%'
    	and tblView.LoaiChungTu like '%'+ @LoaiChungTu + '%'
    	--and ID_NhanVien like @ID_NhanVien
		and exists (select Loai from @tblLoai loai where LoaiSoQuy= loai.Loai)    
		group by tblView.LoaiHoaDon
		) a1
		) b
END");

			Sql(@"ALTER PROCEDURE [dbo].[GetListDebitSalaryDetail]
    @ID_BangLuong [uniqueidentifier],
    @TextSearch [nvarchar](max),
    @Currentpage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	declare @idChiNhanh uniqueidentifier = (select top 1 ID_DonVi from NS_BangLuong where ID= @ID_BangLuong);
    	with data_cte
    	as (
    	select nv.TenNhanVien, quy.*, 
    		case when quy.TamUngLuong > quy.LuongThucNhan - quy.DaTra
				then quy.LuongThucNhan - quy.DaTra else quy.TamUngLuong end as TienTamUngFloat,	
    		quy.LuongThucNhan - quy.DaTra as ConCanTra,
    		iif(quy.LuongThucNhan - quy.TamUngLuong - quy.DaTra> 0, 
			quy.LuongThucNhan - TamUngLuong - DaTra,0) as CanTraSauTamUng
    	from
    	(
    		select 
    			quy_tamung.ID_NhanVien, max(ID_BangLuongChiTiet) as ID_BangLuongChiTiet,
    			max(MaBangLuongChiTiet) as MaBangLuongChiTiet,
    			sum(LuongThucNhan) as LuongThucNhan,
    			sum(DaTra) as DaTra,
    			sum(TamUngLuong) as TamUngLuong
    		from
    			(select quyhd.ID_NhanVien, quyhd.ID_BangLuongChiTiet, quyhd.MaBangLuongChiTiet, quyhd.LuongThucNhan , 
    				sum(quyhd.DaTra) as DaTra, 0 as TamUngLuong
    			from
    			(
    				-- get tienluong datra
    				select blct.ID_NhanVien, blct.ID as ID_BangLuongChiTiet, blct.MaBangLuongChiTiet, blct.LuongThucNhan , 
    					case when qhd.TrangThai= 0 then 0 else sum(isnull(qct.TienThu,0)) + sum(isnull(qct.TruTamUngLuong,0)) end as DaTra		--	
    				from NS_BangLuong_ChiTiet blct
    				left join Quy_HoaDon_ChiTiet qct on blct.ID= qct.ID_BangLuongChiTiet
    				left join Quy_HoaDon qhd on qct.ID_HoaDon= qhd.ID
    				where blct.ID_BangLuong= @ID_BangLuong
    				group by  blct.ID, blct.ID_BangLuong, blct.ID_NhanVien, blct.MaBangLuongChiTiet, blct.LuongThucNhan , qhd.TrangThai
    			) quyhd group by  quyhd.ID_NhanVien, quyhd.ID_BangLuongChiTiet, quyhd.MaBangLuongChiTiet, quyhd.LuongThucNhan
    
    			union all
    			-- todo: get from NS_TamUngLuong (column NoHienTai of NV)
    			select ID_NhanVien,
    				'00000000-0000-0000-0000-000000000000' as ID_BangLuongChiTiet, '' as MaBangLuongChiTiet, 0 as LuongThucNhan, 0 as DaTra, 
    				CongNo as TamUngLuong
    			from NS_CongNoTamUngLuong 
    			where ID_DonVi= @idChiNhanh
    			) quy_tamung group by quy_tamung.ID_NhanVien
    	) quy
    	join NS_NhanVien nv on quy.ID_NhanVien= nv.ID
    	where round(quy.LuongThucNhan - DaTra,0) > 0
    	),
    	count_cte
    	as(
    		select count(ID_NhanVien) as TotalRow,
    			CEILING(COUNT(ID_NhanVien) / CAST(@PageSize as float )) as TotalPage,
    			Sum(LuongThucNhan) as TongLuongNhan,
    			Sum(DaTra) as TongDaTra,
    			Sum(ConCanTra) as TongCanTra,
    			Sum(TamUngLuong) as TongTamUng,
    			Sum(TienTamUngFloat) as TongTruTamUngThucTe,		
    			Sum(CanTraSauTamUng) as TongCanTraSauTamUng		
    		from data_cte
    	)
    	select *,
    		FORMAT(TienTamUngFloat,'###,###.###') as TienTamUng,
    		FORMAT(CanTraSauTamUng,'###,###.###') as TienTra
    	from data_cte dt
    	cross join count_cte ct 
    	order by dt.MaBangLuongChiTiet
    	offset (@CurrentPage * @PageSize) Rows
    	fetch next @PageSize Rows only
END");

			Sql(@"ALTER PROC [dbo].[getListSQ_NhanVien]
@ID_NhanVien [uniqueidentifier]
AS
BEGIN
	Select 
	MaHoaDon,
	NgayLapHoaDon, NguoiNopTien, 
	Case when loaihoadon =  '11' then N'Phiếu thu' else N'Phiếu chi' end as LoaiPhieu,
	CAST (Round(ISNULL(TongTienThu, 0), 0) as float) as TongTienThu
	from
	Quy_HoaDon
	where ID_NhanVien = @ID_NhanVien
	and (TrangThai is null or TrangThai='1')
	ORDER BY NgayLapHoaDon DESC
END
");

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
    	select Name from dbo.splitstring(@IDDonViQuyDois) 
    	insert into @tblIDLoHang
    	select Name from dbo.splitstring(@IDLoHangs) where Name not like '%null%' and Name !=''

		
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
				and hd.ChoThanhToan = 0 AND hd.LoaiHoaDon IN (1, 5, 7, 8, 4, 6, 9, 10,18)
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

			Sql(@"ALTER PROCEDURE [dbo].[GetTPDinhLuong_ofCTHD]
    @ID_CTHD [uniqueidentifier],
	@LoaiHoaDon int null
AS
BEGIN
    SET NOCOUNT ON;

	if @LoaiHoaDon is null
	begin
		-- hoadonban
		select  MaHangHoa, TenHangHoa, ID_DonViQuiDoi, TenDonViTinh, SoLuong, ct.GiaVon, ct.ID_HoaDon,ID_ChiTietGoiDV, ct.ID_LoHang,
			ct.SoLuongDinhLuong_BanDau,
			iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe,
    		case when ISNULL(QuyCach,0) = 0 then ISNULL(TyLeChuyenDoi,1) else ISNULL(QuyCach,0) * ISNULL(TyLeChuyenDoi,1) end as QuyCach,
    		qd.TenDonViTinh as DonViTinhQuyCach, ct.GhiChu	,
			ceiling(qd.GiaNhap) as GiaNhap, qd.GiaBan as GiaBanHH, lo.MaLoHang, lo.NgaySanXuat, lo.NgayHetHan ---- used to nhaphang tu hoadon
    	from BH_HoaDon_ChiTiet ct
    	Join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    	join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
		left join DM_LoHang lo on ct.ID_LoHang = lo.ID
    	where ID_ChiTietDinhLuong = @ID_CTHD and ct.ID != @ID_CTHD
		and ct.SoLuong > 0
		and (ct.ChatLieu is null or ct.ChatLieu !='5')
	end
	else
		-- hdxuatkho co Tpdinhluong
		begin	
		
			-- get thongtin hang xuatkho
			declare @ID_DonViQuiDoi uniqueidentifier, @ID_LoHang uniqueidentifier,  @ID_HoaDonXK uniqueidentifier
			select @ID_DonViQuiDoi= ID_DonViQuiDoi, @ID_LoHang= ID_LoHang, @ID_HoaDonXK = ct.ID_HoaDon
			from BH_HoaDon_ChiTiet ct 
			where ct.ID = @ID_CTHD 


			-- chi get dinhluong thuoc phieu xuatkho nay
			select ct.ID_ChiTietDinhLuong,ct.ID_ChiTietGoiDV, ct.ID_DonViQuiDoi, ct.ID_LoHang,
				ct.SoLuong, ct.DonGia, ct.GiaVon, ct.ThanhTien,ct.ID_HoaDon, ct.GhiChu, ct.ChatLieu,
				qd.MaHangHoa, qd.TenDonViTinh,
				lo.MaLoHang,lo.NgaySanXuat, lo.NgayHetHan,
				hh.TenHangHoa,
				qd.GiaBan,
				qd.ThuocTinhGiaTri as ThuocTinh_GiaTri,
				qd.TenDonViTinh,
				qd.ID_HangHoa,
				hh.QuanLyTheoLoHang,
				hh.LaHangHoa,
				hh.DichVuTheoGio,
				hh.DuocTichDiem,
				ISNULL(qd.LaDonViChuan,'0') as LaDonViChuan, 
				CAST(ISNULL(qd.TyLeChuyenDoi,1) as float) as TyLeChuyenDoi,
				hh.ID_NhomHang as ID_NhomHangHoa, 
				ISNULL(hh.GhiChu,'') as GhiChuHH,
				iif(ct.TenHangHoaThayThe is null or ct.TenHangHoaThayThe ='', hh.TenHangHoa, ct.TenHangHoaThayThe) as TenHangHoaThayThe
			from BH_HoaDon_ChiTiet ct
			Join DonViQuiDoi qd on ct.ID_ChiTietDinhLuong = qd.ID
    		join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
			left join DM_LoHang lo on ct.ID_LoHang= lo.ID
			where ct.ID_DonViQuiDoi= @ID_DonViQuiDoi 
			and ct.ID_HoaDon = @ID_HoaDonXK
			and ((ct.ID_LoHang = @ID_LoHang) or (ct.ID_LoHang is null and @ID_LoHang is null))	
			and (ct.ChatLieu is null or ct.ChatLieu !='5')
		end		
		
END");

			Sql(@"ALTER PROCEDURE [dbo].[import_DoiTuong]
    @MaNhomDoiTuong [nvarchar](max),
    @TenNhomDoiTuong [nvarchar](max),
    @TenNhomDoiTuong_KhongDau [nvarchar](max),
    @TenNhomDoiTuong_KyTuDau [nvarchar](max),
    @MaDoiTuong [nvarchar](max),
    @TenDoiTuong [nvarchar](max),
    @TenDoiTuong_KhongDau [nvarchar](max),
    @TenDoiTuong_ChuCaiDau [nvarchar](max),
    @GioiTinhNam [bit],
    @LoaiDoiTuong [int],
    @LaCaNhan [int],
    @timeCreate [datetime],
    @NgaySinh_NgayTLap [nvarchar](max),
    @DinhDangNgaySinh [nvarchar](max),
    @DiaChi [nvarchar](max),
    @Email [nvarchar](max),
    @Fax [nvarchar](max),
    @web [nvarchar](max),
    @GhiChu [nvarchar](max),
    @DienThoai [nvarchar](max),
    @MaSoThue [nvarchar](max),
    @STK [nvarchar](max),
    @MaHoaDonThu [nvarchar](max),
    @MaHoaDonChi [nvarchar](max),
    @ID_NhanVien [uniqueidentifier],
    @NguoiTao nvarchar(max),
    @ID_DonVi [uniqueidentifier],
    @NoCanThu [nvarchar](max),
    @NoCanTra [nvarchar](max),
	@TongTichDiem [nvarchar](max),
    @MaDieuChinhDiem [nvarchar](max),
	@SoDuThe  [nvarchar](max),
    @MaDieuChinhTheGiaTri [nvarchar](max),
	@TenNguonKhach nvarchar(max),
	@TenTrangThai nvarchar(max)
AS
BEGIN
	set @TenNhomDoiTuong_KhongDau= dbo.FUNC_ConvertStringToUnsign(@TenNhomDoiTuong)
	set @TenDoiTuong_KhongDau= dbo.FUNC_ConvertStringToUnsign(@TenDoiTuong)

	set @TenNhomDoiTuong_KyTuDau= dbo.FUNC_GetStartChar(@TenNhomDoiTuong)
	set @TenDoiTuong_ChuCaiDau= dbo.FUNC_GetStartChar(@TenDoiTuong)

    DECLARE @NoCanThuF as float
    		set @NoCanThuF = (select CAST(ROUND(@NoCanThu, 2) as float))
    DECLARE @NoCanTraF as float
    		set @NoCanTraF = (select CAST(ROUND(@NoCanTra, 2) as float))
	DECLARE @TongTichDiemF as float
    		set @TongTichDiemF = (select CAST(ROUND(@TongTichDiem, 2) as float))
	DECLARE @SoDuTheF as float
    		set @SoDuTheF = (select CAST(ROUND(@SoDuThe, 2) as float))
    DECLARE @NgaySinhTL as Datetime
    	set @NgaySinhTL = null;
    if (len(@NgaySinh_NgayTLap) > 0)
    		Begin
    			Set @NgaySinhTL = (select convert(datetime,@NgaySinh_NgayTLap, 103));
    		end	
    DECLARE @ID_NhomDoiTuong  as uniqueidentifier
    	set @ID_NhomDoiTuong = null
	DECLARE @ID_TrangThai  as uniqueidentifier
    	set @ID_TrangThai = null
	DECLARE @ID_NguonKhach  as uniqueidentifier
    	set @ID_NguonKhach = null
    DECLARE @ID  as uniqueidentifier -- iddoituong
    	SET @ID = NewID();
    DECLARE @ID_QuyHDThu  as uniqueidentifier
    	SET @ID_QuyHDThu = NewID();
    DECLARE @ID_QuyHDTra  as uniqueidentifier
    	SET @ID_QuyHDTra = NewID();
	DECLARE @ID_DieuChinhDiem  as uniqueidentifier
    	SET @ID_DieuChinhDiem = NewID();
	DECLARE @ID_HoaDon  as uniqueidentifier -- dieuchinh thegiatri
    	SET @ID_HoaDon = NewID();


    if (len(@TenNhomDoiTuong) > 0)
    	Begin
    		SET @ID_NhomDoiTuong =  (Select top 1 ID FROM DM_NhomDoiTuong where TenNhomDoiTuong like @TenNhomDoiTuong and LoaiDoiTuong = @LoaiDoiTuong 
			and (TrangThai = 1 or TrangThai is null));
    		if (@ID_NhomDoiTuong is null or len(@ID_NhomDoiTuong) = 0)
    		BeGin
    			SET @ID_NhomDoiTuong = newID();
    			insert into DM_NhomDoiTuong (ID, MaNhomDoiTuong, TenNhomDoiTuong, TenNhomDoiTuong_KhongDau, TenNhomDoiTuong_KyTuDau, LoaiDoiTuong, NguoiTao, NgayTao, TrangThai)
    			values (@ID_NhomDoiTuong, @MaNhomDoiTuong, @TenNhomDoiTuong,@TenDoiTuong_KhongDau, @TenNhomDoiTuong_KyTuDau, @LoaiDoiTuong, @NguoiTao, GETDATE(),1)
    		End
    	End

		if (len(@TenTrangThai) > 0)
    	Begin
    		SET @ID_TrangThai=  (Select top 1 ID FROM DM_DoiTuong_TrangThai where TenTrangThai like  @TenTrangThai)							
    		if (@ID_TrangThai is null or len(@ID_TrangThai) = 0)
    		BeGin
    			SET @ID_TrangThai = newID();
    			insert into DM_DoiTuong_TrangThai (ID, TenTrangThai, NguoiTao, NgayTao)
    			values (@ID_TrangThai, @TenTrangThai, @NguoiTao, GETDATE())
    		End
    	End

		if (len(@TenNguonKhach) > 0)
    	Begin
    		SET @ID_NguonKhach=  (Select top 1 ID FROM DM_NguonKhachHang where TenNguonKhach like   @TenNguonKhach)							
    		if (@ID_NguonKhach is null or len(@ID_NguonKhach) = 0)
    		BeGin
    			SET @ID_NguonKhach = newID();
    			insert into DM_NguonKhachHang (ID, TenNguonKhach, NguoiTao, NgayTao)
    			values (@ID_NguonKhach, @TenNguonKhach, @NguoiTao, GETDATE())
    		End
    	End

    	if (LEN(@DinhDangNgaySinh) < 2)
    	BEGIN
    		set @DinhDangNgaySinh = null;
    	END
    		insert into DM_DoiTuong (ID, LoaiDoiTuong, LaCaNhan, MaDoiTuong, TenDoiTuong,TenDoiTuong_KhongDau, TenDoiTuong_ChuCaiDau,DienThoai, Fax,
    		Email, Website, MaSoThue, TaiKhoanNganHang, GhiChu, NgaySinh_NgayTlap,DinhDang_NgaySinh, chiase, theodoi, DiaChi, GioiTinhNam,
			NguoiTao, NgayTao, ID_DonVi, TongTichDiem, ID_NhanVienPhuTrach, ID_NguonKhach, ID_TrangThai)
    		Values (@ID, @LoaiDoiTuong, @LaCaNhan, @MaDoiTuong, @TenDoiTuong, @TenDoiTuong_KhongDau, @TenDoiTuong_ChuCaiDau, @DienThoai, @Fax,
    		@Email, @web, @MaSoThue,@STK, @GhiChu, @NgaySinhTL,@DinhDangNgaySinh, '0', '0',@DiaChi, @GioiTinhNam,
			@NguoiTao, @timeCreate, @ID_DonVi, @TongTichDiemF,@ID_NhanVien,@ID_NguonKhach , @ID_TrangThai)
    
    		if (@ID_NhomDoiTuong is not null or len (@ID_NhomDoiTuong) > 0)
    		Begin
    			insert into  DM_DoiTuong_Nhom (ID, ID_DoiTuong, ID_NhomDoiTuong)
    			values (NEWID(), @ID, @ID_NhomDoiTuong)
    		End
    	if (@NoCanThuF > 0)
    	Begin
    		insert into Quy_HoaDon (ID, MaHoaDon, NgayLapHoaDon, NgayTao, ID_NhanVien, NguoiNopTien, TongTienThu, ThuCuaNhieuDoiTuong, NguoiTao, ID_DonVi, LoaiHoaDon, HachToanKinhDoanh, PhieuDieuChinhCongNo)
    		values (@ID_QuyHDThu,@MaHoaDonChi,@timeCreate,@timeCreate,@ID_NhanVien,@TenDoiTuong, @NoCanThuF,'0', @NguoiTao, @ID_DonVi, '12','0','1')
    		insert into Quy_HoaDon_ChiTiet (ID, ID_HoaDon, ID_DoiTuong, ThuTuThe, TienMat, TienGui, TienThu, HinhThucThanhToan)
    		values (newID(), @ID_QuyHDThu, @ID, '0', @NoCanThuF, '0', @NoCanThuF, 1)
    	End
		if (@NoCanTraF > 0)
    	Begin
    		insert into Quy_HoaDon (ID, MaHoaDon, NgayLapHoaDon, NgayTao, ID_NhanVien, NguoiNopTien, TongTienThu, ThuCuaNhieuDoiTuong, NguoiTao, ID_DonVi, LoaiHoaDon,HachToanKinhDoanh,PhieuDieuChinhCongNo, TrangThai)
    		values (@ID_QuyHDTra,@MaHoaDonThu,@timeCreate,@timeCreate,@ID_NhanVien,@TenDoiTuong, @NoCanTraF,'0', @NguoiTao, @ID_DonVi, '11','0','1',1)
    		insert into Quy_HoaDon_ChiTiet (ID, ID_HoaDon, ID_DoiTuong, ThuTuThe, TienMat, TienGui, TienThu,HinhThucThanhToan)
    		values (newID(), @ID_QuyHDTra, @ID, '0', @NoCanTraF, '0', @NoCanTraF,1)
    	End
    	if (@TongTichDiemF > 0)
    	Begin
    		insert into Quy_HoaDon (ID, MaHoaDon, NgayLapHoaDon, NgayTao, ID_NhanVien, NguoiNopTien, TongTienThu, ThuCuaNhieuDoiTuong, NguoiTao, ID_DonVi, LoaiHoaDon,NoiDungThu,HachToanKinhDoanh,PhieuDieuChinhCongNo, TrangThai)
    		values (@ID_DieuChinhDiem,@MaDieuChinhDiem,@timeCreate,@timeCreate,@ID_NhanVien,@TenDoiTuong, 0,'0', @NguoiTao, @ID_DonVi, '11',N'Import điều chỉnh điểm','0','1',1)
    		insert into Quy_HoaDon_ChiTiet (ID, ID_HoaDon, ID_DoiTuong, ThuTuThe, TienMat, TienGui, TienThu, DiemThanhToan,GhiChu, HinhThucThanhToan)
    		values (newID(), @ID_DieuChinhDiem, @ID, 0, 0, 0, 0,@TongTichDiemF,N'Import điều chỉnh điểm', 5)
    	End
		if (@SoDuTheF > 0)
    	Begin
    		insert into BH_HoaDon (ID, LoaiHoaDon, MaHoaDon, ID_DoiTuong, ID_DonVi, NgayLapHoaDon, TongTienHang, TongChiPhi, TongGiamGia, TongChietKhau, PhaiThanhToan, TongTienThue, ChoThanhToan, DienGiai)
			values(NEWID(), 23, @MaDieuChinhTheGiaTri, @ID,@ID_DonVi, GETDATE(), @SoDuTheF, @SoDuTheF, 0,0,0,@SoDuTheF,0,N'Import tồn số dư thẻ giá trị')
    	End
		
END");

			Sql(@"ALTER PROCEDURE [dbo].[ListHangHoaTheKhoTheoLoHang]
    @ID_HangHoa [uniqueidentifier],
    @IDChiNhanh [uniqueidentifier],
    @ID_LoHang [uniqueidentifier]
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
		case table1.LoaiHoaDon
			when 10 then case when table1.ID_CheckIn = @IDChiNhanh then N'Nhận chuyển hàng' else N'Chuyển hàng' end
			when 1 then N'Bán hàng'
			when 2 then N'Bảo hành'
			when 4 then N'Nhập hàng'
			when 6 then N'Khách trả hàng'
			when 7 then N'Trả hàng nhập'
			when 8 then N'Xuất kho'
			when 9 then N'Kiểm hàng'
			when 13 then N'Nhập kho nội bộ'
			when 14 then N'Nhập hàng khách thừa'
			when 18 then N'Điều chỉnh giá vốn'
		end as LoaiChungTu
		FROM
	(
    	SELECT hd.ID as ID_HoaDon, 
		hd.MaHoaDon, 
		hd.LoaiHoaDon, 
		hd.YeuCau, 
		hd.ID_CheckIn, 
		hd.ID_DonVi, 
		bhct.ThanhTien * dvqd.TyLeChuyenDoi as ThanhTien,
		bhct.TienChietKhau * dvqd.TyLeChuyenDoi TienChietKhau, 
		CASE WHEN hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4' and hd.LoaiHoaDon = 10 THEN hd.NgaySua ELSE hd.NgayLapHoaDon END as NgayLapHoaDon,    		
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.TonLuyKe, bhct.TonLuyKe_NhanChuyenHang) as TonKho,
		iif(hd.ID_DonVi = @IDChiNhanh, bhct.GiaVon / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi),bhct.GiaVon_NhanChuyenHang / iif(dvqd.TyLeChuyenDoi=0,1,dvqd.TyLeChuyenDoi)) as GiaVon,	
		(case hd.LoaiHoaDon
			when 9 then bhct.SoLuong ---- Số lượng lệch = SLThucTe - TonKhoDB        (-) Giảm  (+): Tăng
			when 10 then
				case when hd.ID_CheckIn= @IDChiNhanh and hd.YeuCau = '4' then bhct.TienChietKhau  ---- da nhanhang
				else iif(hd.YeuCau = '4',- bhct.TienChietKhau,- bhct.SoLuong) end 
			--- xuat
			when 1 then - bhct.SoLuong
			when 2 then - bhct.SoLuong ---- hoadon baohanh
			when 7 then - bhct.SoLuong
			when 8 then - bhct.SoLuong
			--- conlai: nhap
			else bhct.SoLuong end
		) * dvqd.TyLeChuyenDoi as SoLuong
    	FROM BH_HoaDon hd
    	LEFT JOIN BH_HoaDon_ChiTiet bhct on hd.ID = bhct.ID_HoaDon
    	LEFT JOIN DonViQuiDoi dvqd on bhct.ID_DonViQuiDoi = dvqd.ID
    	LEFT JOIN DM_HangHoa hh on dvqd.ID_HangHoa = hh.ID		
    	WHERE bhct.ID_LoHang = @ID_LoHang 
		and hd.LoaiHoaDon not in (3,19,25,31, 29)  --- 29.khoitao phutungxe
		and hh.ID = @ID_HangHoa 
		and hd.ChoThanhToan = 0 
		and (bhct.ChatLieu is null or bhct.ChatLieu!='2')
		and ((hd.ID_DonVi = @IDChiNhanh 
		and ((hd.YeuCau != '2' and hd.YeuCau != '3') or hd.YeuCau is null)) or (hd.ID_CheckIn = @IDChiNhanh and hd.YeuCau = '4'))
	) as table1
    group by ID_HoaDon, MaHoaDon, NgayLapHoaDon,LoaiHoaDon, ID_DonVi, ID_CheckIn
	ORDER BY NgayLapHoaDon desc
END");

			Sql(@"ALTER PROCEDURE [dbo].[ReportDiscountInvoice]
    @ID_ChiNhanhs [nvarchar](max),
    @ID_NhanVienLogin [nvarchar](max),
    @TextSearch [nvarchar](max),
	@LoaiChungTus [nvarchar](max),
    @DateFrom [nvarchar](max),
    @DateTo [nvarchar](max),
    @Status_ColumHide [int],
    @StatusInvoice [int],
    @Status_DoanhThu [int],
    @CurrentPage [int],
    @PageSize [int]
AS
BEGIN
    SET NOCOUNT ON;
    	set @DateTo = dateadd(day,1, @DateTo) 

		declare @tblChiNhanh table (ID uniqueidentifier)
    	insert into @tblChiNhanh
    	select * from dbo.splitstring(@ID_ChiNhanhs)
    
    	declare @nguoitao nvarchar(100) = (select top 1 taiKhoan from HT_NguoiDung where ID_NhanVien= @ID_NhanVienLogin)
    	declare @tblNhanVien table (ID uniqueidentifier)
    	insert into @tblNhanVien
    	select * from dbo.GetIDNhanVien_inPhongBan(@ID_NhanVienLogin, @ID_ChiNhanhs,'BCCKHoaDon_XemDS_PhongBan','BCCKHoaDon_XemDS_HeThong');

		declare @tblChungTu table (LoaiChungTu int)
    	insert into @tblChungTu
    	select Name from dbo.splitstring(@LoaiChungTus)
    	DECLARE @tblSearchString TABLE (Name [nvarchar](max));
    DECLARE @count int;
    INSERT INTO @tblSearchString(Name) select  Name from [dbo].[splitstringByChar](@TextSearch, ' ') where Name!='';
    Select @count =  (Select count(*) from @tblSearchString);
    
    	declare @tblDiscountInvoice table (ID uniqueidentifier, MaNhanVien nvarchar(50), TenNhanVien nvarchar(max), 
    		HoaHongThucThu float, HoaHongDoanhThu float, HoaHongVND float, TongAll float)
    	
    	-- bang tam chua DS phieu thu theo Ngay timkiem
    	select qct.ID_HoaDonLienQuan, SUM(qct.TienThu) as ThucThu, qhd.NgayLapHoaDon, qhd.ID
    	into #temp
    	from Quy_HoaDon_ChiTiet qct
    	join (
    			select qhd.ID, qhd.NgayLapHoaDon
    			from Quy_HoaDon qhd
    			join BH_NhanVienThucHien th on qhd.ID= th.ID_QuyHoaDon
    			where (qhd.TrangThai is null or qhd.TrangThai = '1')
    			and qhd.ID_DonVi in (select ID from @tblChiNhanh)
    			and qhd.NgayLapHoaDon >= @DateFrom
    			and qhd.NgayLapHoaDon < @DateTo
    			group by qhd.ID, qhd.NgayLapHoaDon) qhd on qct.ID_HoaDon = qhd.ID
    	where (qct.HinhThucThanhToan is null or qct.HinhThucThanhToan != 4)
    	group by qct.ID_HoaDonLienQuan, qhd.NgayLapHoaDon, qhd.ID;
    	
    	select ID, MaNhanVien, 
    			TenNhanVien,
    		SUM(ISNULL(HoaHongThucThu,0.0)) as HoaHongThucThu,
    		SUM(ISNULL(HoaHongDoanhThu,0.0)) as HoaHongDoanhThu,
    		SUM(ISNULL(HoaHongVND,0.0)) as HoaHongVND,			
    		case @Status_ColumHide
    			when  1 then cast(0 as float)
    			when  2 then SUM(ISNULL(HoaHongVND,0.0))
    			when  3 then SUM(ISNULL(HoaHongThucThu,0.0))
    			when  4 then SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    			when  5 then SUM(ISNULL(HoaHongDoanhThu,0.0)) 
    			when  6 then SUM(ISNULL(HoaHongDoanhThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    			when  7 then SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongDoanhThu,0.0))
    		else SUM(ISNULL(HoaHongThucThu,0.0)) + SUM(ISNULL(HoaHongDoanhThu,0.0)) + SUM(ISNULL(HoaHongVND,0.0))
    		end as TongAll
    		into #temp2
    	from 
    	(
    		select nv.ID, MaNhanVien, TenNhanVien,
    			case when TinhChietKhauTheo =1 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau  end end as HoaHongThucThu,
    				case when TinhChietKhauTheo =3 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end as HoaHongVND,
    				-- neu HD tao thang truoc, nhung PhieuThu thuoc thang nay: HoaHongDoanhThu = 0
    				case when hd.NgayLapHoaDon between @DateFrom and @DateTo and hd.ID_DonVi in (select ID from @tblChiNhanh) then
    					case when TinhChietKhauTheo = 2 then case when hd.LoaiHoaDon = 6 then -TienChietKhau else TienChietKhau end end else 0 end as HoaHongDoanhThu,
    				-- timkiem theo NgayLapHD or NgayLapPhieuThu
    				case when @DateFrom <= hd.NgayLapHoaDon and  hd.NgayLapHoaDon < @DateTo then hd.NgayLapHoaDon else tblQuy.NgayLapHoaDon end as NgayLapHoaDon,
    				case when hd.ChoThanhToan='0' then 1 else 2 end as TrangThaiHD
    		from BH_NhanVienThucHien th
    		join BH_HoaDon hd on th.ID_HoaDon= hd.ID
    		join NS_NhanVien nv on th.ID_NhanVien= nv.ID
    			left join #temp tblQuy on hd.ID= tblQuy.ID_HoaDonLienQuan and (th.ID_QuyHoaDon= tblQuy.ID)	
    		where th.ID_HoaDon is not null
    		and hd.ChoThanhToan  is not null
    			and hd.ID_DonVi in (select * from dbo.splitstring(@ID_ChiNhanhs))  
				and (exists (select LoaiChungTu from @tblChungTu ctu where ctu.LoaiChungTu = hd.LoaiHoaDon))
    			and (exists (select ID from @tblNhanVien nv where th.ID_NhanVien = nv.ID) or hd.NguoiTao like @nguoitao)
    			-- chi lay CKDoanhThu hoac CKThucThu/VND exist in Quy_HoaDon or (not exist QuyHoaDon but LoaiHoaDon =6 )
    			and (th.TinhChietKhauTheo != 1 or (th.TinhChietKhauTheo =1 and ( exists (select ID from #temp where th.ID_QuyHoaDon = #temp.ID) or  LoaiHoaDon=6)))
    			and
    				((select count(Name) from @tblSearchString b where     			
    				nv.TenNhanVien like '%'+b.Name+'%'
    				or nv.TenNhanVienKhongDau like '%'+b.Name+'%'
    				or nv.TenNhanVienChuCaiDau like '%'+b.Name+'%'
    				or nv.MaNhanVien like '%'+b.Name+'%'				
    				)=@count or @count=0)	
    	) tbl
    		where tbl.NgayLapHoaDon >= @DateFrom and tbl.NgayLapHoaDon < @DateTo and TrangThaiHD = @StatusInvoice
    	group by MaNhanVien, TenNhanVien, ID
    	--	having SUM(ISNULL(HoaHongThucThu,0)) + SUM(ISNULL(HoaHongDoanhThu,0)) + SUM(ISNULL(HoaHongVND,0)) > 0 -- chi lay NV co CK > 0
    		
    		if @Status_DoanhThu =0
    			insert into @tblDiscountInvoice
    			select *
    			from #temp2 
    		else
    			begin
    				if @Status_DoanhThu= 1
    					insert into @tblDiscountInvoice
    					select *
    					from #temp2 where HoaHongDoanhThu > 0 or HoaHongThucThu > 0
    				else
    					if @Status_DoanhThu= 2
    						insert into @tblDiscountInvoice
    						select *
    						from #temp2 where HoaHongDoanhThu > 0 or HoaHongVND > 0
    					else		
    						if @Status_DoanhThu= 3
    							insert into @tblDiscountInvoice
    							select *
    							from #temp2 where HoaHongDoanhThu > 0
    						else	
    							if @Status_DoanhThu= 4
    								insert into @tblDiscountInvoice
    								select *
    								from #temp2 where HoaHongVND > 0 Or HoaHongThucThu > 0
    							else
    								if @Status_DoanhThu= 5
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2 where  HoaHongThucThu > 0
    								else -- 6
    									insert into @tblDiscountInvoice
    									select *
    									from #temp2  where HoaHongVND > 0
    								
    			end;
    			
    		with data_cte
    		as(
    		select * from @tblDiscountInvoice
    		),
    		count_cte
    		as (
    			select count(ID) as TotalRow,
    				CEILING(COUNT(ID) / CAST(@PageSize as float ))  as TotalPage,
    				sum(HoaHongDoanhThu) as TongHoaHongDoanhThu,
    				sum(HoaHongThucThu) as TongHoaHongThucThu,
    				sum(HoaHongVND) as TongHoaHongVND,
    				sum(TongAll) as TongAllAll
    			from data_cte
    		)
    		select dt.*, cte.*
    		from data_cte dt
    		cross join count_cte cte
    		order by dt.MaNhanVien
    		OFFSET (@CurrentPage* @PageSize) ROWS
    		FETCH NEXT @PageSize ROWS ONLY
END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateCongNo_TamUngLuong]
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
    			from Quy_HoaDon_ChiTiet qct1 where exists (select top 1 ID from @tblQuyChiTiet qct2 where qct1.ID= qct2.ID_QuyChiTiet)
    
    			declare @giamtruLuong bit= (
    				select TinhLuong
    				from Quy_KhoanThuChi khoan
    				where id= @idKhoanThuChi)
    
    			if @giamtruLuong ='1'
    			begin
    				if (select count(ID) from NS_CongNoTamUngLuong  where ID_NhanVien = @nvTamUng and ID_DonVi = @ID_ChiNhanh) = 0	
    					-- neu chua tontai: insert
    					insert into NS_CongNoTamUngLuong
    					values(NEWID(), @ID_ChiNhanh, @nvTamUng,@sotienTamUng)
    				else
    					-- update
    					update NS_CongNoTamUngLuong set CongNo= CongNo + @sotienTamUng where ID_NhanVien = @nvTamUng and ID_DonVi= @ID_ChiNhanh
    			end
    			
    		end
    	else
    		begin
    			update tblNoCu set CongNo= tblNoHienTai.NoHienTai
    			from NS_CongNoTamUngLuong tblNoCu
    			join (
				-- hoac: thanhtoanluong tru tamung
    				select congno.ID,congno.CongNo - quy.TruTamUngLuong as NoHienTai
    				from NS_CongNoTamUngLuong congno
    				join (
    					select qct.ID_NhanVien, qct.TruTamUngLuong
    					from Quy_HoaDon_ChiTiet qct 
    					join @tblQuyChiTiet qct2 on qct.ID= qct2.ID_QuyChiTiet			
    					) quy on congno.ID_NhanVien= quy.ID_NhanVien
    				where congno.ID_DonVi= @ID_ChiNhanh
    			) tblNoHienTai on tblNoCu.ID= tblNoHienTai.ID
    		end
END

--UpdateCongNo_TamUngLuong 'D93B17EA-89B9-4ECF-B242-D03B8CDE71DE','69e47615-7a1a-4506-8181-20c024e2122a','0'");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateStatusBangLuong_whenChangeCong]
    @ID_DonVi [uniqueidentifier],
    @NgayChamCong [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    	set @NgayChamCong = FORMAT(@NgayChamCong,'yyyy-MM-dd')
    
    	update bl1 set bl1.TrangThai= 2
		from NS_BangLuong bl1 
    	where exists (select ID
    					from
    						(select ID, FORMAT(TuNgay,'yyyy-MM-dd') as TuNgay, FORMAT(DenNgay,'yyyy-MM-dd') as DenNgay
    						from NS_BangLuong
    						where TrangThai = 1 and ID_DonVi= @ID_DonVi
    						) bl
    					where bl.TuNgay<= @ngaychamcong and bl.DenNgay >= @ngaychamcong and bl1.ID= bl.ID)
END");

			Sql(@"ALTER PROCEDURE [dbo].[UpdateTonLuyKeCTHD_whenUpdate]
    @IDHoaDonInput [uniqueidentifier],
    @IDChiNhanhInput [uniqueidentifier],
    @NgayLapHDOld [datetime]
AS
BEGIN
    SET NOCOUNT ON;
    
    		DECLARE @NgayLapHDNew DATETIME;   
    		DECLARE @NgayNhanHang DATETIME;
    		declare @tblHoaDonChiTiet ChiTietHoaDonEdit -- table user defined
    		DECLARE @IDCheckIn  UNIQUEIDENTIFIER, @YeuCau NVARCHAR(MAX),  @LoaiHoaDon INT, @NgayLapHDMin DATETIME;
    		DECLARE @tblChiTiet TABLE (ID_HangHoa UNIQUEIDENTIFIER not null, ID_LoHang UNIQUEIDENTIFIER null, ID_DonViQuiDoi UNIQUEIDENTIFIER not null, TyLeChuyenDoi float not null)
    		DECLARE @LuyKeDauKy TABLE(ID_LoHang UNIQUEIDENTIFIER, ID_HangHoa UNIQUEIDENTIFIER, TonLuyKe FLOAT);
    		DECLARE @hdctUpdate TABLE(ID UNIQUEIDENTIFIER, ID_DonVi UNIQUEIDENTIFIER, ID_CheckIn UNIQUEIDENTIFIER, TonLuyKe FLOAT, LoaiHoaDon INT, 
    		MaHoaDon nvarchar(max), NgayLapHoaDon datetime, YeuCau nvarchar(max));
    
    		--  get NgayLapHD by IDHoaDon: if update HDNhanHang (loai 10, yeucau = 4 --> get NgaySua
    		select 
    			@NgayLapHDNew = NgayLapHoaDon,
    			@NgayNhanHang = NgaySua,
    			@LoaiHoaDon = LoaiHoaDon, @YeuCau = YeuCau, @IDCheckIn = ID_CheckIn
    		from (
    					select LoaiHoaDon, YeuCau, ID_CheckIn, ID_DonVi, NgaySua, 
						case when @IDChiNhanhInput = ID_CheckIn and YeuCau !='1' then NgaySua else NgayLapHoaDon end as NgayLapHoaDon
    					from BH_HoaDon where ID = @IDHoaDonInput) a
    
    		-- alway get Ngay min --> compare to update TonLuyKe
    		IF(@NgayLapHDOld > @NgayLapHDNew)
    			SET @NgayLapHDMin = @NgayLapHDNew;
    		ELSE
    			SET @NgayLapHDMin = @NgayLapHDOld;
    
			declare @NgayLapHDMin_SubMiliSecond datetime = dateadd(MILLISECOND,-2, @NgayLapHDMin)

    		-- get cthd update by IDHoaDon
    		INSERT INTO @tblChiTiet
    		SELECT 
    			qd.ID_HangHoa, ct.ID_LoHang, ct.ID_DonViQuiDoi, qd.TyLeChuyenDoi
    		FROM BH_HoaDon_ChiTiet ct
    		INNER JOIN BH_HoaDon hd ON hd.ID = ct.ID_HoaDon			
    		INNER JOIN DonViQuiDoi qd ON qd.ID = ct.ID_DonViQuiDoi			
    		INNER JOIN DM_HangHoa hh on hh.ID = qd.ID_HangHoa    		
    		WHERE hd.ID = @IDHoaDonInput AND hh.LaHangHoa = 1 
    		GROUP BY qd.ID_HangHoa,ct.ID_DonViQuiDoi,qd.TyLeChuyenDoi, ct.ID_LoHang, hd.ID_DonVi, hd.ID_CheckIn, hd.YeuCau, hd.NgaySua, hd.NgayLapHoaDon;	
    		insert into @tblHoaDonChiTiet select * from @tblChiTiet			
    				
    		-- get cthd has KiemKe group by ID_HangHoa, ID_LoHang
    		declare @tblHangKiemKe table (NgayKiemKe datetime, ID_HangHoa uniqueidentifier null, ID_LoHang uniqueidentifier null)
    		insert into @tblHangKiemKe
    		select NgayLapHoaDon, qd.ID_HangHoa, ct.ID_LoHang
    			from BH_HoaDon_ChiTiet ct 
    			join BH_HoaDon hd ON hd.ID = ct.ID_HoaDon		
    			join DonViQuiDoi qd on ct.ID_DonViQuiDoi= qd.ID    			
    			WHERE hd.ChoThanhToan = 0
    			and hd.LoaiHoaDon= 9
    			and hd.ID_DonVi = @IDChiNhanhInput and NgayLapHoaDon > @NgayLapHDMin_SubMiliSecond
				and exists (select * from @tblChiTiet tblct where qd.ID_HangHoa = tblct.ID_HangHoa AND (ct.ID_LoHang = tblct.ID_LoHang OR ct.ID_LoHang IS NULL)	)
    			group by qd.ID_HangHoa, ct.ID_LoHang, hd.NgayLapHoaDon				
    		
    		-- get cthd liên quan
    		select
    			ct.ID, 
    			ct.ID_LoHang,
				-- chatlieu = 5 (cthd bi xoa khi updateHD), chatlieu =2 (tra gdv  - khong cong lai tonkho)
    			case when ct.ChatLieu= '5' or ct.ChatLieu ='2' then 0 else SoLuong end as SoLuong, 
    			case when ct.ChatLieu= '5' then 0 else TienChietKhau end as TienChietKhau,
    			case when ct.ChatLieu= '5' then 0 else ct.ThanhTien end as ThanhTien,-- kiemke bi huy
    			case when hd.LoaiHoaDon= 10 and  hd.YeuCau = '4' AND hd.ID_CheckIn = @IDChiNhanhInput then ct.TonLuyKe_NhanChuyenHang else ct.TonLuyKe end as TonDauKy,
    			qd.ID_HangHoa,
    			qd.TyLeChuyenDoi,
    			hd.MaHoaDon,
    			hd.LoaiHoaDon,
    			hd.ID_DonVi,
    			hd.ID_CheckIn,
    			hd.YeuCau,								
    			case when hd.YeuCau = '4' AND hd.ID_CheckIn = @IDChiNhanhInput then hd.NgaySua else hd.NgayLapHoaDon end as NgayLapHoaDon
    		into #temp
    		from BH_HoaDon_ChiTiet ct
    		join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
    		join BH_HoaDon hd on ct.ID_HoaDon = hd.ID   	
    		WHERE (hd.ChoThanhToan = 0 or (hd.LoaiHoaDon= 3 and hd.ChoThanhToan= '1'))	-- used to tao BG (chuaduyet), sau do clcik Duyet
    		AND (hd.ID_DonVi = @IDChiNhanhInput OR (hd.ID_CheckIn = @IDChiNhanhInput AND hd.YeuCau = '4'))
			and exists (select * from @tblChiTiet ctupdate where qd.ID_HangHoa = ctupdate.ID_HangHoa AND (ct.ID_LoHang = ctupdate.ID_LoHang OR ct.ID_LoHang IS NULL))

		
    		-- table cthd has ID_HangHoa exist cthd kiemke
    		declare @cthdHasKiemKe table (ID_HangHoa uniqueidentifier, ID_LoHang uniqueidentifier)
    		declare @tblNgayKiemKe table (NgayKiemKe datetime)
    
    		declare @count float= (select count(*) from @tblHangKiemKe)   	
    			begin						
    				declare @ID_HangHoa uniqueidentifier, @ID_LoHang uniqueidentifier				
    				DECLARE Cur_tblKiemKe CURSOR SCROLL LOCAL FOR
    				select ID_HangHoa, ID_LoHang
    				from @tblHangKiemKe
    				order by NgayKiemKe
    
    				OPEN Cur_tblKiemKe -- cur 1
    			FETCH FIRST FROM Cur_tblKiemKe
    				INTO @ID_HangHoa, @ID_LoHang
    				WHILE @@FETCH_STATUS = 0
    				BEGIN	
    						if not exists (select * from @cthdHasKiemKe kk where kk.ID_HangHoa= @ID_HangHoa and (kk.ID_LoHang= @ID_LoHang OR kk.ID_LoHang is null))
    							begin
    								-- get list NgayKiemKe by ID_HangHoa & ID_LoHang								
    								declare @NgayKiemKe datetime
    								declare @FromDate datetime = @NgayLapHDMin
    
    								-- get cac khoang thoigian kiemke
    								insert into @tblNgayKiemKe
    								select *
    								from
    									( select NgayKiemKe 
    									from @tblHangKiemKe kk where kk.ID_HangHoa = @ID_HangHoa and (kk.ID_LoHang= @ID_LoHang or kk.ID_LoHang is null)						
    									union 
    										select GETDATE() as NgayKiemKe
    									) b order by NgayKiemKe
    
    								DECLARE Cur_NgayKiemKe CURSOR SCROLL LOCAL FOR								
    								select NgayKiemKe from @tblNgayKiemKe
    
    								OPEN Cur_NgayKiemKe -- cur 2
    							FETCH FIRST FROM Cur_NgayKiemKe
    								INTO @NgayKiemKe
    								WHILE @@FETCH_STATUS = 0
    									begin											
    										insert into @cthdHasKiemKe values(@ID_HangHoa, @ID_LoHang)
    										-- get tondauky 
    										if @FromDate = @NgayLapHDMin and @LoaiHoaDon !=9		
    											begin
    												insert into @LuyKeDauKy
    												select 
    													ID_LoHang,ID_HangHoa,TonDauKy																		
    												from
    													(
    													select 
    														ID_LoHang,ID_HangHoa,TonDauKy,										
    														ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    													from #temp
    													where NgayLapHoaDon < @FromDate		
    													and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang OR #temp.ID_LoHang IS NULL)										
    													) luyke	
    												where luyke.RN= 1									
    											end
    										else
    											begin
    												insert into @LuyKeDauKy
    												select 
    													ID_LoHang,ID_HangHoa,TonDauKy
    												from
    													(
    													select 
    														ID_LoHang,ID_HangHoa,TonDauKy,
    														ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    													from #temp
    													where NgayLapHoaDon <=  @FromDate 
    													and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang OR #temp.ID_LoHang IS NULL)		
    													) luyke	
    												where luyke.RN= 1
    											end
    		
    										--- tinh lai tonluyke
    										INSERT INTO @hdctUpdate
    										select ID, ID_DonVi, ID_CheckIn,
    												ISNULL(lkdk.TonLuyKe, 0) + 
    												(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * a.SoLuong* a.TyLeChuyenDoi, 
    											IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    												IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    											IIF(a.LoaiHoaDon = 10 AND a.YeuCau = '4' AND a.ID_CheckIn = @IDChiNhanhInput, a.TienChietKhau* a.TyLeChuyenDoi, 0))))) 
    												OVER(PARTITION BY a.ID_HangHoa, a.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe,
    												LoaiHoaDon, MaHoaDon,NgayLapHoaDon, YeuCau
    										from
    											(							
    											select 
    												ID,
    												ID_LoHang,
    												SoLuong,
    												TienChietKhau,
    												ThanhTien,
    												ID_HangHoa,
    												TyLeChuyenDoi,
    												MaHoaDon,
    												LoaiHoaDon,
    												NgayLapHoaDon,
    												ID_DonVi,
    												ID_CheckIn,
    												YeuCau
    											from #temp
    											where NgayLapHoaDon >= @FromDate
    												and NgayLapHoaDon < @NgayKiemKe
    												and #temp.ID_HangHoa = @ID_HangHoa AND (#temp.ID_LoHang = @ID_LoHang or #temp.ID_LoHang IS NULL	)						
    											) a
    										LEFT JOIN @LuyKeDauKy lkdk ON lkdk.ID_HangHoa = a.ID_HangHoa AND (lkdk.ID_LoHang = a.ID_LoHang OR a.ID_LoHang IS NULL)	
    						
    										-- xóa TonLuyKe trước đó để lấy TonLuyKe mới theo khoảng thời gian		
    										set @FromDate = @NgayKiemKe
    										--select *, 1 as after1 from @LuyKeDauKy
    										delete from @LuyKeDauKy															
    										FETCH NEXT FROM Cur_NgayKiemKe INTO @NgayKiemKe
    									end
    								CLOSE Cur_NgayKiemKe  
    								DEALLOCATE Cur_NgayKiemKe 
    							end		
    
    						-- delete & assign again in for loop
    						delete from @tblNgayKiemKe
    						FETCH NEXT FROM Cur_tblKiemKe INTO @ID_HangHoa,@ID_LoHang
    					END
    				CLOSE Cur_tblKiemKe  
    				DEALLOCATE Cur_tblKiemKe 				
    			end
    
    			-- get luyke dauky of HangHoa not exist in ctkiemke
    			begin
    				insert into @LuyKeDauKy
    				select 
    					ID_LoHang,ID_HangHoa,TonDauKy											
    				from
    					(
    					select 
    						ID_LoHang,ID_HangHoa,TonDauKy,
    						ROW_NUMBER() OVER (PARTITION BY ID_HangHoa, ID_LoHang ORDER BY NgayLapHoaDon DESC) AS RN 
    					from #temp
    					where NgayLapHoaDon < @NgayLapHDMin 
    						and not exists (select * from @tblHangKiemKe kk where #temp.ID_HangHoa =  kk.ID_HangHoa and (#temp.ID_LoHang = kk.ID_LoHang OR #temp.ID_LoHang is null))
    					) luyke	
    				where luyke.RN= 1
    
    				-- caculator again TonLuyKe for all cthd 'liên quan'
    				INSERT INTO @hdctUpdate
    				select ID, ID_DonVi, ID_CheckIn,
    						ISNULL(lkdk.TonLuyKe, 0) + 
    						(SUM(IIF(LoaiHoaDon IN (1, 5, 7, 8), -1 * a.SoLuong* a.TyLeChuyenDoi, 
    					IIF(LoaiHoaDon IN (4, 6, 18,13,14), SoLuong * TyLeChuyenDoi, 				
    						IIF((LoaiHoaDon = 10 AND YeuCau = '1') OR (ID_CheckIn IS NOT NULL AND ID_CheckIn != @IDChiNhanhInput AND LoaiHoaDon = 10 AND YeuCau = '4') AND ID_DonVi = @IDChiNhanhInput, -1 * TienChietKhau* TyLeChuyenDoi, 				
    					IIF(a.LoaiHoaDon = 10 AND a.YeuCau = '4' AND a.ID_CheckIn = @IDChiNhanhInput, a.TienChietKhau* a.TyLeChuyenDoi, 0))))) 
    						OVER(PARTITION BY a.ID_HangHoa, a.ID_LoHang ORDER BY NgayLapHoaDon)) AS TonLuyKe,
    						LoaiHoaDon, MaHoaDon,NgayLapHoaDon,YeuCau
    				from
    					(
    					select 
    						ID,
    						ID_LoHang,
    						SoLuong,
    						TienChietKhau,
    						ThanhTien,
    						ID_HangHoa,
    						TyLeChuyenDoi,
    						MaHoaDon,
    						LoaiHoaDon,
    						NgayLapHoaDon,
    						ID_DonVi,
    						ID_CheckIn,
    						YeuCau
    					from #temp
    					where NgayLapHoaDon > @NgayLapHDMin_SubMiliSecond
    					and not exists (select * from @tblHangKiemKe kk where #temp.ID_HangHoa =  kk.ID_HangHoa and (#temp.ID_LoHang = kk.ID_LoHang OR #temp.ID_LoHang is null))
    					) a
    				LEFT JOIN @LuyKeDauKy lkdk ON lkdk.ID_HangHoa = a.ID_HangHoa AND (lkdk.ID_LoHang = a.ID_LoHang OR a.ID_LoHang IS NULL)					
    			end
    		
    		--select *, 1 as after2 from @LuyKeDauKy
    		--select * , @NgayLapHDMin as NgayMin from @hdctUpdate order by NgayLapHoaDon desc
			begin try
    			UPDATE hdct
    		SET hdct.TonLuyKe = IIF(tlkupdate.ID_DonVi = @IDChiNhanhInput, tlkupdate.TonLuyKe, hdct.TonLuyKe), 
    			hdct.TonLuyKe_NhanChuyenHang = IIF(tlkupdate.ID_CheckIn = @IDChiNhanhInput and tlkupdate.LoaiHoaDon = 10, tlkupdate.TonLuyKe, hdct.TonLuyKe_NhanChuyenHang)
    		FROM BH_HoaDon_ChiTiet hdct
    		INNER JOIN @hdctUpdate tlkupdate ON hdct.ID = tlkupdate.ID where tlkupdate.LoaiHoaDon !=9 -- don't update TonLuyKe of HD KiemKe
    
    		-- get TonKho hientai full ID_QuiDoi, ID_LoHang of ID_HangHoa
    		DECLARE @tblTonKho1 TABLE(ID_DonViQuiDoi UNIQUEIDENTIFIER, TonKho FLOAT, ID_LoHang UNIQUEIDENTIFIER)
    		INSERT INTO @tblTonKho1
    		SELECT qd.ID, [dbo].[FUNC_TonLuyKeTruocThoiGian](@IDChiNhanhInput,qd.ID_HangHoa,ID_LoHang, DATEADD(HOUR, 1,GETDATE()))/qd.TyLeChuyenDoi as TonKho, ID_LoHang 
    		FROM @tblChiTiet ct
    		join DonViQuiDoi qd on ct.ID_HangHoa = qd.ID_HangHoa 
    		
    		--select * from @tblTonKho1
    
    		-- UPDATE TonKho in DM_HangHoa_TonKho
    		UPDATE hhtonkho SET hhtonkho.TonKho = ISNULL(cthoadon.TonKho, 0)
    		FROM DM_HangHoa_TonKho hhtonkho
    		INNER JOIN @tblTonKho1 as cthoadon on hhtonkho.ID_DonViQuyDoi = cthoadon.ID_DonViQuiDoi 
    			and (hhtonkho.ID_LoHang = cthoadon.ID_LoHang or cthoadon.ID_LoHang is null) and hhtonkho.ID_DonVi = @IDChiNhanhInput

				--- insert row into DM_HangHoa_TonKho if not exists
			INSERT INTO DM_HangHoa_TonKho(ID, ID_DonVi, ID_DonViQuyDoi, ID_LoHang, TonKho)
			SELECT NEWID(), @IDChiNhanhInput, cthoadon1.ID_DonViQuiDoi, cthoadon1.ID_LoHang, cthoadon1.TonKho
			FROM @tblTonKho1 AS cthoadon1
			LEFT JOIN DM_HangHoa_TonKho hhtonkho1 on hhtonkho1.ID_DonViQuyDoi = cthoadon1.ID_DonViQuiDoi 
			and (hhtonkho1.ID_LoHang = cthoadon1.ID_LoHang or cthoadon1.ID_LoHang is null) and hhtonkho1.ID_DonVi = @IDChiNhanhInput
			WHERE hhtonkho1.ID IS NULL
			end try
			begin catch
			end catch
	
			begin try
				exec Insert_ThongBaoHetTonKho @IDChiNhanhInput, @LoaiHoaDon, @tblHoaDonChiTiet
			end try
			begin catch
			end catch

    
    	
    		-- neu update NhanHang --> goi ham update TonKho 2 lan
    		-- update GiaVon neu tontai phieu NhapHang,ChuyenHang/NhanHang, DieuChinhGiaVon 
    		declare @count2 float = (select count(ID) from @hdctUpdate where LoaiHoaDon in (4,7,10, 18))
    		select ISNULL(@count2,0) as UpdateGiaVon, ISNULL(@count,0) as UpdateKiemKe, @NgayLapHDMin as NgayLapHDMin

		
	


END");

			Sql(@"ALTER PROCEDURE [dbo].[XuatKhoToanBo_FromHoaDonSC]
    @ID_HoaDon [uniqueidentifier] 
AS
BEGIN
    SET NOCOUNT ON;

	-- count cthd la hanghoa
		declare @tongGtriXuat float, @count int
		select @count = count(ct.ID) , @tongGtriXuat= sum(ct.GiaVon * SoLuong)
			from BH_HoaDon_ChiTiet ct 
			join DonViQuiDoi qd on ct.ID_DonViQuiDoi = qd.ID
			join DM_HangHoa hh on qd.ID_HangHoa= hh.ID
			where ct.ID_HoaDon= @ID_HoaDon
			and hh.LaHangHoa = 1 and (ct.ChatLieu is null or ct.ChatLieu !='5')
		
		IF @count > 0
		BEGIN
				---- INSERT HD XUATKHO ----
			
			 declare @ID_XuatKho uniqueidentifier = newID()	
			 declare @ngaylapHD datetime, @ID_DonVi uniqueidentifier,  @mahoadon nvarchar(max)
    		select @ngaylapHD = NgayLapHoaDon, @ID_DonVi = ID_DonVi from BH_HoaDon where id= @ID_HoaDon
    		declare @ngayxuatkho datetime = dateadd(millisecond,2,@ngaylapHD)

				---- get mahoadon xuatkho
    			declare @tblMa table (MaHoaDon nvarchar(max))
    			insert into @tblMa
    			exec GetMaHoaDonMax_byTemp 8, @ID_DonVi, @ngaylapHD
    			select @mahoadon = MaHoaDon from @tblMa

				insert into BH_HoaDon (ID, LoaiHoaDon, MaHoaDon, ID_HoaDon,ID_PhieuTiepNhan, NgayLapHoaDon, ID_DonVi, ID_NhanVien, TongTienHang, TongThanhToan, TongChietKhau, TongChiPhi, TongGiamGia, TongTienThue, 
    			PhaiThanhToan, PhaiThanhToanBaoHiem, ChoThanhToan, YeuCau, NgayTao, NguoiTao, DienGiai)
    			select @ID_XuatKho, 8, @mahoadon,@ID_HoaDon,ID_PhieuTiepNhan, @ngayxuatkho, @ID_DonVi,ID_NhanVien, @tongGtriXuat,0,0,0,0,0, @tongGtriXuat,0,0,N'Hoàn thành', GETDATE(), NguoiTao, 
				concat(N'Xuất kho toàn bộ từ hóa đơn ', MaHoaDon)
    			from BH_HoaDon 
    			where id= @ID_HoaDon

    
			---- INSERT CT XUATKHO -----
		
		
			insert into BH_HoaDon_ChiTiet (ID, ID_HoaDon, SoThuTu, ID_ChiTietDinhLuong, ID_ChiTietGoiDV, 
						ID_DonViQuiDoi, ID_LoHang, SoLuong, DonGia, GiaVon, ThanhTien, ThanhToan, 
    					PTChietKhau, TienChietKhau, PTChiPhi, TienChiPhi, TienThue, An_Hien, TonLuyKe, GhiChu, TenHangHoaThayThe, ChatLieu)				
			select 
				NEWid(),
				@ID_XuatKho,
				row_number() over( order by (select 1)) as SoThuTu,
				ctsc.ID_ChiTietDinhLuong,
				ctsc.ID_ChiTietGoiDV,
				ctsc.ID_DonViQuiDoi,
				ctsc.ID_LoHang,
				ctsc.SoLuong, ctsc.GiaVon, ctsc.GiaVon, ctsc.GiaTri, 
				0,0,0,0,0,0,'1', ctsc.TonLuyKe, ctsc.GhiChu, ctsc.TenHangHoaThayThe, ctsc.ChatLieu
			from 
			(
			--- ct hoadon suachua
				select 
					cttp.ID as ID_ChiTietGoiDV,
					isnull(ctdv.ID_DichVu,cttp.ID_DonViQuiDoi) as ID_ChiTietDinhLuong, -- id_hanghoa/id_dichvu
					cttp.SoLuong,
					cttp.GiaVon,
					cttp.GiaVon* cttp.SoLuong as GiaTri,
					cttp.ID_DonViQuiDoi,
					cttp.ID_LoHang,
					cttp.TonLuyKe,
					isnull(cttp.GhiChu,'') as GhiChu,
					isnull(cttp.TenHangHoaThayThe,'') as TenHangHoaThayThe,
					cttp.ChatLieu -- chatlieu = 4. check sudung gdv
				from BH_HoaDon_ChiTiet cttp
				left join
				(
					select ctm.ID_DonViQuiDoi as ID_DichVu, ctm.ID, ctm.ID_LichBaoDuong
					from BH_HoaDon_ChiTiet ctm where ctm.ID_HoaDon= @ID_HoaDon
					and ctm.SoLuong > 0
				) ctdv on cttp.ID_ChiTietDinhLuong = ctdv.ID
				where cttp.ID_HoaDon= @ID_HoaDon
				and (cttp.ChatLieu is null or cttp.ChatLieu !='5')
				and cttp.SoLuong > 0
				and cttp.ID_LichBaoDuong is null ---- khong xuat hang bao duong
				) ctsc
				JOIN DonViQuiDoi qd on ctsc.ID_DonViQuiDoi = qd.ID
				JOIN DM_HangHoa hh on qd.ID_HangHoa = hh.ID
				where hh.LaHangHoa ='1'


			select @ID_XuatKho as ID_HoaDon,  @ngayxuatkho as NgayLapHoaDon ---- get ngaylaphd of hdsc --> insert diary & update tonkho

			--exec UpdateTonLuyKeCTHD_whenUpdate @ID_XuatKho,@ID_DonVi,@ngayxuatkho
		END
		
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
				cast(c.TienThue as float) as TienThue,	
				cast(c.TienChietKhau as float) as TienChietKhau,
				cast(c.ThanhTienTruocCK as float) as ThanhTienTruocCK,
				iif(@XemGiaVon='1',cast(c.TienVon as float),0) as TienVon,
				cast(c.ThanhTien - c.GiamGiaHD as float) as DoanhThu,
				iif(@XemGiaVon='1',cast(c.ThanhTien - c.GiamGiaHD - c.TienVon as float),0) as LaiLo		,
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
				sum(b.TienThue) as TienThue,
				sum(b.TienChietKhau) as TienChietKhau,
				sum(b.ThanhTienTruocCK) as ThanhTienTruocCK
			from (
			select 					
				a.ID_LoHang, a.ID_DonViQuiDoi,									
				sum(isnull(a.TienThue,0)) as TienThue,
				sum(isnull(a.GiamGiaHD,0)) as GiamGiaHD,
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
        }
        
        public override void Down()
        {
            Sql("DROP FUNCTION [dbo].[FN_GetMaHangHoa]");
			DropStoredProcedure("[dbo].[HD_GetBuTruTraHang]");
			DropStoredProcedure("[dbo].[TinhCongNo_HDTra]");
        }
    }
}
