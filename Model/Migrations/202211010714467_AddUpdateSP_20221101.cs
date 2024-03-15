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
        }
        
        public override void Down()
        {
            Sql("DROP FUNCTION [dbo].[FN_GetMaHangHoa]");
			DropStoredProcedure("[dbo].[HD_GetBuTruTraHang]");
			DropStoredProcedure("[dbo].[TinhCongNo_HDTra]");
        }
    }
}
